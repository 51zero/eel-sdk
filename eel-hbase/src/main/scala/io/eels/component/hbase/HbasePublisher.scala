package io.eels.component.hbase

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.sksamuel.exts.io.Using
import com.sksamuel.exts.metrics.Timed
import io.eels.Row
import io.eels.datastream.{Publisher, Subscriber, Subscription}
import io.eels.schema.StructType
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Result, Scan}

import scala.collection.mutable.ArrayBuffer

class HbasePublisher(connection: Connection,
                     schema: StructType,
                     namespace: String,
                     tableName: String,
                     bufferSize: Int,
                     maxRows: Long,
                     scanner: Scan,
                     implicit val serializer: HbaseSerializer) extends Publisher[Seq[Row]] with Timed with Using {

  private val table = connection.getTable(TableName.valueOf(namespace, tableName))

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {
    try {
      using(new CloseableIterator) { rowIter =>
        val running = new AtomicBoolean(true)
        subscriber.subscribed(Subscription.fromRunning(running))
        val buffer = new ArrayBuffer[Row](bufferSize)
        while (rowIter.hasNext && running.get()) {
          buffer append rowIter.next()
          if (buffer.size == bufferSize) {
            subscriber.next(buffer.toVector)
            buffer.clear()
          }
        }
        if (buffer.nonEmpty) subscriber.next(buffer.toVector)
        subscriber.completed()
      }
    } catch {
      case t: Throwable => subscriber.error(t)
    }
  }

  class CloseableIterator extends Iterator[Row] with AutoCloseable {
    private val resultScanner = table.getScanner(scanner)
    private val resultScannerIter = resultScanner.iterator()
    private var rowCount = 0
    private var iter: Iterator[Row] = Iterator.empty

    override def hasNext: Boolean = rowCount < maxRows && iter.hasNext || {
      if (rowCount < maxRows && resultScannerIter.hasNext) {
        iter = HBaseResultsIterator(schema, resultScannerIter)
        iter.hasNext
      } else false
    }

    override def next(): Row = {
      rowCount += 1
      iter.next()
    }

    override def close(): Unit = {
      resultScanner.close()
    }
  }

  case class HBaseResultsIterator(schema: StructType, resultIter: util.Iterator[Result])(implicit serializer: HbaseSerializer) extends Iterator[Row] {
    override def hasNext: Boolean = resultIter.hasNext

    override def next(): Row = {
      val resultRow = resultIter.next()
      val values = schema.fields.map { field =>
        if (!field.key) {
          val value = resultRow.getValue(field.columnFamily.getOrElse(sys.error(s"No Column Family defined for field '${field.name}'")).getBytes, field.name.getBytes)
          if (value != null) serializer.fromBytes(value, field.name, field.dataType) else null
        } else serializer.fromBytes(resultRow.getRow, field.name, field.dataType)
      }
      Row(schema, values)
    }
  }


}
