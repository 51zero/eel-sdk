package io.eels.component.hive

import com.typesafe.config.ConfigFactory
import io.eels.util.Logging
import io.eels.schema.Schema
import io.eels.Sink
import io.eels.SinkWriter
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

data class HiveSink(private val dbName: String,
                    private val tableName: String,
                    private val ioThreads: Int = 4,
                    private val dynamicPartitioning: Boolean?,
                    private val schemaEvolution: Boolean?,
                    val fs: FileSystem,
                    val client: IMetaStoreClient) : Sink, Logging {

  val config = ConfigFactory.load()
  val includePartitionsInData = config.getBoolean("eel.hive.includePartitionsInData")
  val bufferSize = config.getInt("eel.hive.bufferSize")
  val SchemaEvolutionDefault = config.getBoolean("eel.hive.sink.schemaEvolution")
  val DynamicPartitioningDefault = config.getBoolean("eel.hive.sink.dynamicPartitioning")

  fun withIOThreads(ioThreads: Int): HiveSink = copy(ioThreads = ioThreads)
  fun withDynamicPartitioning(partitioning: Boolean): HiveSink = copy(dynamicPartitioning = partitioning)
  fun withSchemaEvolution(schemaEvolution: Boolean): HiveSink = copy(schemaEvolution = schemaEvolution)

  private fun dialect(client: IMetaStoreClient): HiveDialect {
    val format = HiveOps.tableFormat(dbName, tableName)
    logger.debug("Table format is $format")
    return io.eels.component.hive.HiveDialect(format)
  }

  private fun containsUpperCase(schema: Schema): Boolean = schema.columnNames().all { it == it.toLowerCase() }

  override fun writer(schema: Schema): SinkWriter {
    if (containsUpperCase(schema)) {
      logger.warn("Writing to hive with a schema that contains upper case characters, but hive will lower case all field names. This might lead to subtle case bugs. It is recommended, but not required, that you explicitly convert schemas to lower case before serializing to hive")
    }

    if (schemaEvolution.getOrElse(SchemaEvolutionDefault)) {
      HiveSchemaEvolve(dbName, tableName, schema)
    }

    return HiveSinkWriter(
      schema,
      HiveOps.schema(dbName, tableName),
      dbName,
      tableName,
      ioThreads,
      dialect,
      dynamicPartitioning.getOrElse(DynamicPartitioningDefault),
      includePartitionsInData,
      bufferSize
    )
  }
}
