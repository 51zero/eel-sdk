package io.eels.component.hbase

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Row, SinkWriter}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{BufferedMutator, _}

object HbaseSinkWriter extends Logging {

  def apply(namespace: String,
            table: String,
            numberOfWriters: AtomicInteger,
            schema: StructType,
            maxKeyValueSize: Option[Int],
            writeBufferSize: Option[Long],
            writeRowBatchSize: Int,
            serializer: HbaseSerializer,
            connection: Connection): Seq[HbaseSinkWriter] = {
    val tableName = TableName.valueOf(namespace, table)

    /** A callback invoked when an asynchronous write fails. */
    val listener = new BufferedMutator.ExceptionListener {
      override def onException(exception: RetriesExhaustedWithDetailsException, mutator: BufferedMutator): Unit = {
        for (i <- 0 until exception.getNumExceptions) {
          logger.error("Failed to send put " + exception.getRow(i) + ".")
        }
      }
    }

    /** Shared HBase write buffer for N writer threads */
    val params = new BufferedMutatorParams(tableName).listener(listener)
    writeBufferSize.map(params.writeBufferSize)
    maxKeyValueSize.map(params.maxKeyValueSize)
    val mutator = connection.getBufferedMutator(params)
    val rowCounter = new AtomicLong(0)
    for (_ <- 0 until numberOfWriters.get())
      yield new HbaseSinkWriter(schema, rowCounter, numberOfWriters, mutator, writeRowBatchSize, connection, serializer)
  }
}

class HbaseSinkWriter(schema: StructType,
                      rowCounter: AtomicLong,
                      numberOfWriters: AtomicInteger,
                      mutator: BufferedMutator,
                      writeRowBatchSize: Int,
                      connection: Connection,
                      serializer: HbaseSerializer) extends SinkWriter with Logging {

  /** Cache row key information from the schema */
  private val rowKeyIndex = schema.fields.indexWhere(_.key)
  if (rowKeyIndex == -1) sys.error("HBase requires a single column to be defined as a key")
  private val keyField = schema.fields(rowKeyIndex)

  /** Fields in the schema with their column positions */
  private val fieldsWithIndex = schema.fields.zipWithIndex

  override def write(row: Row): Unit = {
    if ((rowCounter.incrementAndGet() % writeRowBatchSize) == 0) mutator.flush()
    val rowKey = serializer.toBytes(row.values(rowKeyIndex), keyField.name, keyField.dataType)
    val put = new Put(rowKey)
    for ((field, index) <- fieldsWithIndex) {
      if (index != rowKeyIndex && row.values(index) != null) {
        val cf = field.columnFamily.getOrElse(sys.error(s"No Column Family defined for field '${field.name}'")).getBytes
        val col = field.name.getBytes()
        put.addColumn(cf, col, serializer.toBytes(row.values(index), field.name, field.dataType))
      }
    }
    mutator.mutate(put)
  }

  override def close(): Unit = {
    mutator.flush()
    if (numberOfWriters.decrementAndGet() == 0) mutator.close()
  }

}
