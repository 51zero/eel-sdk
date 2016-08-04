package io.eels.component.hive

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.eels.util.Logging
import io.eels.schema.Schema
import io.eels.Sink
import io.eels.SinkWriter
import io.eels.util.Option
import io.eels.util.getOrElse
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.IMetaStoreClient

data class HiveSink @JvmOverloads constructor(private val dbName: String,
                                              private val tableName: String,
                                              val fs: FileSystem = FileSystem.get(Configuration()),
                                              val client: IMetaStoreClient = HiveMetaStoreClient(HiveConf()),
                                              private val ioThreads: Int = 4,
                                              private val dynamicPartitioning: Option<Boolean> = Option.None,
                                              private val schemaEvolution: Option<Boolean> = Option.None) : Sink, Logging {

  val config: Config = ConfigFactory.load()
  val includePartitionsInData = config.getBoolean("eel.hive.includePartitionsInData")
  val bufferSize = config.getInt("eel.hive.bufferSize")
  val SchemaEvolutionDefault = config.getBoolean("eel.hive.sink.schemaEvolution")
  val DynamicPartitioningDefault = config.getBoolean("eel.hive.sink.dynamicPartitioning")

  val ops = HiveOps(client)

  fun withIOThreads(ioThreads: Int): HiveSink = copy(ioThreads = ioThreads)
  fun withDynamicPartitioning(partitioning: Boolean): HiveSink = copy(dynamicPartitioning = Option(partitioning))
  fun withSchemaEvolution(schemaEvolution: Boolean): HiveSink = copy(schemaEvolution = Option(schemaEvolution))

  private fun dialect(): HiveDialect {
    val format = ops.tableFormat(dbName, tableName)
    logger.debug("Table format is $format")
    return io.eels.component.hive.HiveDialect(format)
  }

  private fun containsUpperCase(schema: Schema): Boolean = schema.fieldNames().all { it == it.toLowerCase() }

  override fun writer(schema: Schema): SinkWriter {
    if (containsUpperCase(schema)) {
      logger.warn("Writing to hive with a schema that contains upper case characters is discouraged because Hive will lowercase all the values. This might lead to subtle case bugs. It is recommended, but not required, that you explicitly convert schemas to lower case before serializing to hive")
    }

    if (schemaEvolution.getOrElse(SchemaEvolutionDefault)) {
      // HiveSchemaEvolve(dbName, tableName, schema)
      throw UnsupportedOperationException("Schema evolution is not yet implemented")
    }

    val metastoreSchema = ops.schema(dbName, tableName)

    return HiveSinkWriter(
        schema,
        metastoreSchema,
        dbName,
        tableName,
        ioThreads,
        dialect(),
        dynamicPartitioning.getOrElse(DynamicPartitioningDefault),
        includePartitionsInData,
        bufferSize,
        fs,
        client
    )
  }
}
