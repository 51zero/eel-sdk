package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.typesafe.config.{Config, ConfigFactory}
import io.eels.{Sink, SinkWriter}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType}
import org.apache.hadoop.security.UserGroupInformation
import com.sksamuel.exts.OptionImplicits._
import io.eels.schema.StructType

object HiveSink {
  val CaseErrorMsg = "Writing to hive with a schema that contains upper case characters is discouraged because Hive will lowercase all the values. This might lead to subtle case bugs. It is recommended, but not required, that you explicitly convert schemas to lower case before serializing to hive"

  val config: Config = ConfigFactory.load()
  val bufferSize = config.getInt("eel.hive.bufferSize")
  val schemaEvolutionDefault = config.getBoolean("eel.hive.sink.schemaEvolution")
  val dynamicPartitioningDefault = config.getBoolean("eel.hive.sink.dynamicPartitioning")
  val errorOnUpperCase = config.getBoolean("eel.hive.sink.errorOnUpperCase")
}

case class HiveSink(dbName: String,
                    tableName: String,
                    ioThreads: Int = 4,
                    dynamicPartitioning: Option[Boolean] = None,
                    schemaEvolution: Option[Boolean] = None,
                    permission: Option[FsPermission] = None,
                    inheritPermissions: Option[Boolean] = None,
                    principal: Option[String] = None,
                    keytabPath: Option[java.nio.file.Path] = None,
                    fileListener: FileListener = FileListener.noop,
                    createTable: Boolean = false,
                    metadata: Map[String, String] = Map.empty)
                   (implicit fs: FileSystem, client: IMetaStoreClient) extends Sink with Logging {

  import HiveSink._

  implicit val conf = fs.getConf
  val ops = new HiveOps(client)

  def withIOThreads(ioThreads: Int): HiveSink = copy(ioThreads = ioThreads)
  def withCreateTable(createTable: Boolean): HiveSink = copy(createTable = createTable)
  def withDynamicPartitioning(partitioning: Boolean): HiveSink = copy(dynamicPartitioning = Some(partitioning))
  def withSchemaEvolution(schemaEvolution: Boolean): HiveSink = copy(schemaEvolution = Some(schemaEvolution))
  def withPermission(permission: FsPermission): HiveSink = copy(permission = Option(permission))
  def withInheritPermission(inheritPermissions: Boolean): HiveSink = copy(inheritPermissions = Option(inheritPermissions))
  def withFileListener(listener: FileListener): HiveSink = copy(fileListener = listener)
  def withMetaData(map: Map[String, String]): HiveSink = copy(metadata = map)

  def withKeytabFile(principal: String, keytabPath: java.nio.file.Path): HiveSink = {
    login()
    copy(principal = principal.some, keytabPath = keytabPath.some)
  }

  private def dialect(): HiveDialect = {
    login()
    val format = ops.tableFormat(dbName, tableName)
    logger.debug(s"Table format is $format; detecting dialect...")
    io.eels.component.hive.HiveDialect(format)
  }

  private def login(): Unit = {
    for (user <- principal; path <- keytabPath) {
      UserGroupInformation.loginUserFromKeytab(user, path.toString)
    }
  }

  def containsUpperCase(schema: StructType): Boolean = schema.fieldNames().exists(name => name.exists(Character.isUpperCase))

  override def writer(schema: StructType): SinkWriter = {
    login()

    if (containsUpperCase(schema)) {
      if (errorOnUpperCase)
        sys.error(HiveSink.CaseErrorMsg)
      else
        logger.warn(HiveSink.CaseErrorMsg)
    }

    if (createTable) {
      if (!ops.tableExists(dbName, tableName)) {
        ops.createTable(dbName, tableName, schema, schema.partitions.map(_.name.toLowerCase), HiveFormat.Parquet, Map.empty, TableType.MANAGED_TABLE)
      }
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
      bufferSize,
      inheritPermissions,
      permission,
      fileListener,
      metadata
    )
  }
}