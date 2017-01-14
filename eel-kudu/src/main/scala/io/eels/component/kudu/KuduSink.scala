package io.eels.component.kudu

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Row, Sink, SinkWriter}
import org.apache.kudu.client.{CreateTableOptions, KuduClient}

import scala.collection.JavaConverters._

case class KuduSink(tableName: String)(implicit client: KuduClient) extends Sink with Logging {

  override def writer(structType: StructType): SinkWriter = new SinkWriter {

    val schema = KuduSchemaFns.toKuduSchema(structType)

    if (client.tableExists(tableName))
      client.deleteTable(tableName)

    if (!client.tableExists(tableName)) {
      logger.debug(s"Creating table $tableName")
      val options = new CreateTableOptions()
        .setNumReplicas(1)
        .setRangePartitionColumns(structType.fields.filter(_.key).map(_.name).asJava)
      client.createTable(tableName, schema, options)
    }

    override def write(row: Row): Unit = ()
    override def close(): Unit = ()
  }
}

object KuduSink {
  def apply(master: String, table: String): KuduSink = {
    implicit val client = new KuduClient.KuduClientBuilder(master).build()
    KuduSink(table)
  }
}
