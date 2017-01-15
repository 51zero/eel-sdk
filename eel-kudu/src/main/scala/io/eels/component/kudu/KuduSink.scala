package io.eels.component.kudu

import com.sksamuel.exts.Logging
import io.eels.schema._
import io.eels.{Row, Sink, SinkWriter}
import org.apache.kudu.client.{CreateTableOptions, KuduClient}

import scala.collection.JavaConverters._

case class KuduSink(tableName: String)(implicit client: KuduClient) extends Sink with Logging {

  override def writer(structType: StructType): SinkWriter = new SinkWriter {

    val schema = KuduSchemaFns.toKuduSchema(structType)

    if (client.tableExists(tableName))
      client.deleteTable(tableName)

    val table = if (!client.tableExists(tableName)) {
      logger.debug(s"Creating table $tableName")
      val options = new CreateTableOptions()
        .setNumReplicas(1)
        .setRangePartitionColumns(structType.fields.filter(_.key).map(_.name).asJava)
      client.createTable(tableName, schema, options)
    } else {
      client.openTable(tableName)
    }

    val session = client.newSession()

    override def write(row: Row): Unit = {
      val insert = table.newInsert()
      val partial = insert.getRow
      for ((field, index) <- row.schema.fields.zipWithIndex) {
        val value = row.values(index)
        if (value == null) {
          partial.setNull(index)
        } else {
          field.dataType match {
            case StringType => KuduStringWriter.write(partial, index, value)
            case _: LongType => KuduLongWriter.write(partial, index, value)
            case _: IntType => KuduIntWriter.write(partial, index, value)
            case _: ShortType => KuduShortWriter.write(partial, index, value)
            case _: ByteType => KuduByteWriter.write(partial, index, value)
            case BooleanType => KuduBooleanWriter.write(partial, index, value)
            case FloatType => KuduFloatWriter.write(partial, index, value)
            case DoubleType => KuduDoubleWriter.write(partial, index, value)
            case BinaryType => KuduBinaryWriter.write(partial, index, value)
          }
        }
      }
      session.apply(insert)
    }

    override def close(): Unit = {
      session.close()
    }
  }
}

object KuduSink {
  def apply(master: String, table: String): KuduSink = {
    implicit val client = new KuduClient.KuduClientBuilder(master).build()
    KuduSink(table)
  }
}