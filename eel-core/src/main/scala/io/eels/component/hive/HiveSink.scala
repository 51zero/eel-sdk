package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import io.eels.schema.Schema
import io.eels.{Sink, SinkWriter}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

object HiveSink {
  val CaseErrorMsg = "Writing to hive with a schema that contains upper case characters is discouraged because Hive will lowercase all the values. This might lead to subtle case bugs. It is recommended, but not required, that you explicitly convert schemas to lower case before serializing to hive"
}

case class HiveSink(dbName: String,
                    tableName: String,
                    ioThreads: Int = 4,
                    dynamicPartitioning: Option[Boolean] = None,
                    schemaEvolution: Option[Boolean] = None)
                   (implicit fs: FileSystem, client: IMetaStoreClient) extends Sink with Logging {

  val config: Config = ConfigFactory.load()
  val includePartitionsInData = config.getBoolean("eel.hive.includePartitionsInData")
  val bufferSize = config.getInt("eel.hive.bufferSize")
  val schemaEvolutionDefault = config.getBoolean("eel.hive.sink.schemaEvolution")
  val dynamicPartitioningDefault = config.getBoolean("eel.hive.sink.dynamicPartitioning")
  val errorOnUpperCase = config.getBoolean("eel.hive.sink.errorOnUpperCase")

  val ops = new HiveOps(client)

  def withIOThreads(ioThreads: Int): HiveSink = copy(ioThreads = ioThreads)
  def withDynamicPartitioning(partitioning: Boolean): HiveSink = copy(dynamicPartitioning = Some(partitioning))
  def withSchemaEvolution(schemaEvolution: Boolean): HiveSink = copy(schemaEvolution = Some(schemaEvolution))

  private def dialect(): HiveDialect = {
    val format = ops.tableFormat(dbName, tableName)
    logger.debug(s"Table format is $format")
    io.eels.component.hive.HiveDialect(format)
  }

  private def containsUpperCase(schema: Schema): Boolean = schema.fieldNames.forall { it => it == it.toLowerCase() }

  override def writer(schema: Schema): SinkWriter = {
    if (containsUpperCase(schema)) {
      if (errorOnUpperCase)
        sys.error(HiveSink.CaseErrorMsg)
      else
        logger.warn(HiveSink.CaseErrorMsg)
    }

    if (schemaEvolution.contains(true) || schemaEvolutionDefault) {
      // HiveSchemaEvolve(dbName, tableName, schema)
      throw new UnsupportedOperationException("Schema evolution is not yet implemented")
    }

    val metastoreSchema = ops.schema(dbName, tableName)

    new HiveSinkWriter(
      schema,
      metastoreSchema,
      dbName,
      tableName,
      ioThreads,
      dialect(),
      dynamicPartitioning.contains(true) || dynamicPartitioningDefault,
      includePartitionsInData,
      bufferSize
    )
  }
}
