package io.eels.component.hive

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Schema, Sink, SinkWriter}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.JavaConverters._

case class HiveSink(private val dbName: String,
                    private val tableName: String,
                    private val ioThreads: Int = 4,
                    private val dynamicPartitioning: Boolean = true,
                    private val schemaEvolution: Boolean = true)
                   (implicit fs: FileSystem, hiveConf: HiveConf) extends Sink with StrictLogging {

  val config = ConfigFactory.load()
  val includePartitionsInData = config.getBoolean("eel.hive.includePartitionsInData")
  val bufferSize = config.getInt("eel.hive.bufferSize")
  // val schemaEvolution = config.getBoolean("eel.hive.sink.schemaEvolution")

  def withIOThreads(ioThreads: Int): HiveSink = copy(ioThreads = ioThreads)
  def withDynamicPartitioning(dynamicPartitioning: Boolean): HiveSink = copy(dynamicPartitioning = dynamicPartitioning)
  def withSchemaEvolution(schemaEvolution: Boolean): HiveSink = copy(schemaEvolution = schemaEvolution)

  private def hiveSchema(implicit client: HiveMetaStoreClient): Schema = {
    val schema = client.getSchema(dbName, tableName)
    HiveSchemaFns.fromHiveFields(schema.asScala)
  }

  private def dialect(implicit client: HiveMetaStoreClient): HiveDialect = {
    val format = HiveOps.tableFormat(dbName, tableName)
    logger.debug(s"Table format is $format")
    HiveDialect(format)
  }

  override def writer(schema: Schema): SinkWriter = {

    implicit val client = new HiveMetaStoreClient(hiveConf)

    if (schemaEvolution) {
      HiveSchemaEvolve(dbName, tableName, schema)
    }

    new HiveSinkWriter(
      schema,
      hiveSchema,
      dbName,
      tableName,
      ioThreads,
      dialect,
      dynamicPartitioning,
      includePartitionsInData,
      bufferSize
    )
  }
}

object HiveSink {

  @deprecated("functionality should move to the HiveSinkBuilder", "0.33.0")
  def apply(dbName: String, tableName: String, params: Map[String, List[String]])
           (implicit fs: FileSystem, hiveConf: HiveConf): HiveSink = {
    val dynamicPartitioning = params.get("dynamicPartitioning").map(_.head).getOrElse("false") == "true"
    HiveSink(
      dbName,
      tableName,
      dynamicPartitioning = dynamicPartitioning
    )
  }
}