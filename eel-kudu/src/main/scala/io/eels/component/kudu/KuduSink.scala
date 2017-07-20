package io.eels.component.kudu

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import io.eels.schema._
import io.eels.{Row, Sink, SinkWriter}
import org.apache.kudu.client.{CreateTableOptions, KuduClient}

import scala.collection.JavaConverters._

object KuduSinkConfig {
  def apply(): KuduSinkConfig = apply(ConfigFactory.load())
  def apply(config: Config): KuduSinkConfig = KuduSinkConfig(
    WriteMode.valueOf(config.getString("eel.kudu.write-mode"))
  )
}
case class KuduSinkConfig(writeMode: WriteMode)

case class KuduSink(tableName: String,
                    config: KuduSinkConfig)(implicit client: KuduClient) extends Sink with Logging {

  override def open(structType: StructType): SinkWriter = new SinkWriter {

    val schema = KuduSchemaFns.toKuduSchema(structType)

    private def deleteTable(): Unit = if (client.tableExists(tableName)) client.deleteTable(tableName)
    private def createTable(): Unit = {
      if (!client.tableExists(tableName)) {
        logger.debug(s"Creating table $tableName")
        val options = new CreateTableOptions()
          .setNumReplicas(1)
          .setRangePartitionColumns(structType.fields.filter(_.key).map(_.name).asJava)
        client.createTable(tableName, schema, options)
      }
    }


    config.writeMode match {
      case WriteMode.OVERWRITE =>
        deleteTable()
        createTable()
      case WriteMode.CREATE =>
        createTable()
      case _ =>
    }

    val table = client.openTable(tableName)
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
            case BinaryType => KuduBinaryWriter.write(partial, index, value)
            case BooleanType => KuduBooleanWriter.write(partial, index, value)
            case _: ByteType => KuduByteWriter.write(partial, index, value)
            case DoubleType => KuduDoubleWriter.write(partial, index, value)
            case FloatType => KuduFloatWriter.write(partial, index, value)
            case _: IntType => KuduIntWriter.write(partial, index, value)
            case _: LongType => KuduLongWriter.write(partial, index, value)
            case _: ShortType => KuduShortWriter.write(partial, index, value)
            case StringType => KuduStringWriter.write(partial, index, value)
            case TimeMicrosType => KuduLongWriter.write(partial, index, value)
            case TimeMillisType => KuduLongWriter.write(partial, index, value)
            case TimestampMillisType => KuduLongWriter.write(partial, index, value)
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
  def apply(master: String, table: String, config: KuduSinkConfig = KuduSinkConfig()): KuduSink = {
    implicit val client = new KuduClient.KuduClientBuilder(master).build()
    KuduSink(table, config)
  }
}