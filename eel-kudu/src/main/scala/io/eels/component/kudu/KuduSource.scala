package io.eels.component.kudu

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.datastream.{DataStream, Publisher, Subscriber}
import io.eels.schema._
import io.eels.{Row, Source}
import org.apache.kudu.client.{KuduClient, KuduScanner, RowResultIterator}

import scala.collection.JavaConverters._

case class KuduSource(tableName: String)(implicit client: KuduClient) extends Source with Logging {

  override lazy val schema: StructType = {
    val schema = client.openTable(tableName).getSchema
    KuduSchemaFns.fromKuduSchema(schema)
  }

  override def parts(): Seq[Publisher[Seq[Row]]] = Seq(new KuduPublisher(tableName))

  class KuduPublisher(tableName: String) extends Publisher[Seq[Row]] with Using {

    override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {

      val projectColumns = schema.fieldNames()
      val table = client.openTable(tableName)

      val scanner = client.newScannerBuilder(table)
        .setProjectedColumnNames(projectColumns.asJava)
        .build

      try {
        val iterator = new ScannerIterator(scanner, schema)
        iterator.grouped(DataStream.DefaultBatchSize).foreach(subscriber.next)
        subscriber.completed()
      } catch {
        case t: Throwable => subscriber.error(t)
      } finally {
        scanner.close()
      }
    }
  }
}

object ResultsIterator {
  def apply(schema: StructType, iter: RowResultIterator) = new Iterator[Row] {

    private val zipped = schema.fields.zipWithIndex

    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = {
      val next = iter.next()
      val values = zipped.map { case (field, index) =>
        field.dataType match {
          case BinaryType => BinaryValueReader.read(next, index)
          case BooleanType => BooleanValueReader.read(next, index)
          case _: ByteType => ByteValueReader.read(next, index)
          case DoubleType => DoubleValueReader.read(next, index)
          case FloatType => FloatValueReader.read(next, index)
          case _: IntType => IntValueReader.read(next, index)
          case _: LongType => LongValueReader.read(next, index)
          case _: ShortType => ShortValueReader.read(next, index)
          case StringType => StringValueReader.read(next, index)
          case TimeMicrosType => LongValueReader.read(next, index)
          case TimeMillisType => LongValueReader.read(next, index)
          case TimestampMillisType => LongValueReader.read(next, index)
        }
      }
      Row(schema, values)
    }
  }
}

class ScannerIterator(scanner: KuduScanner, schema: StructType) extends Iterator[Row] {
  var iter: Iterator[Row] = Iterator.empty
  override def hasNext: Boolean = iter.hasNext || {
    if (scanner.hasMoreRows) {
      iter = ResultsIterator(schema, scanner.nextRows)
      iter.hasNext
    } else {
      false
    }
  }
  override def next(): Row = iter.next()
}

object KuduSource {
  def apply(master: String, table: String): KuduSource = {
    implicit val client = new KuduClient.KuduClientBuilder(master).build()
    KuduSource(table)
  }
}
