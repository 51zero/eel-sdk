package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.typesafe.config.{Config, ConfigFactory}
import io.eels.component.hive.dialect.ParquetHiveDialect
import io.eels.component.hive.partition.{DynamicPartitionStrategy, PartitionStrategy}
import io.eels.schema.StructType
import io.eels.{Sink, SinkWriter}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType}
import org.apache.hadoop.security.UserGroupInformation

import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode.RoundingMode

object HiveSink {
  private val config: Config = ConfigFactory.load()
  private val upperCaseAction = config.getString("eel.hive.sink.upper-case-action")
}

case class HiveSink(dbName: String,
                    tableName: String,
                    permission: Option[FsPermission] = None,
                    inheritPermissions: Option[Boolean] = None,
                    principal: Option[String] = None,
                    partitionFields: Seq[String] = Nil,
                    partitionStrategy: PartitionStrategy = new DynamicPartitionStrategy,
                    filenameStrategy: FilenameStrategy = DefaultFilenameStrategy,
                    stagingStrategy: StagingStrategy = DefaultStagingStrategy,
                    metastoreSchemaHandler: MetastoreSchemaHandler = RequireCompatibilityMetastoreSchemaHandler,
                    alignStrategy: AlignmentStrategy = RowPaddingAlignmentStrategy,
                    outputSchemaStrategy: OutputSchemaStrategy = SkipPartitionsOutputSchemaStrategy,
                    keytabPath: Option[java.nio.file.Path] = None,
                    fileListener: FileListener = FileListener.noop,
                    createTable: Boolean = false,
                    // dialect used to create a new table, for existing tables the dialect will always be detected
                    dialect: Option[HiveDialect] = None,
                    callbacks: Seq[CommitCallback] = Nil,
                    roundingMode: RoundingMode = RoundingMode.UNNECESSARY,
                    metadata: Map[String, String] = Map.empty)
                   (implicit fs: FileSystem, client: IMetaStoreClient) extends Sink with Logging {

  import HiveSink._

  implicit private val conf = fs.getConf
  private val ops = new HiveOps(client)

  def withCreateTable(createTable: Boolean,
                      partitionFields: Seq[String] = Nil,
                      dialect: HiveDialect = ParquetHiveDialect()): HiveSink =
    copy(createTable = createTable, partitionFields = partitionFields, dialect = dialect.some)

  def withPermission(permission: FsPermission): HiveSink = copy(permission = Option(permission))
  def withInheritPermission(inheritPermissions: Boolean): HiveSink = copy(inheritPermissions = Option(inheritPermissions))
  def withPartitionFields(first: String, rest: String*): HiveSink = copy(partitionFields = first +: rest)
  def withPartitionFields(partitionFields: Seq[String]): HiveSink = copy(partitionFields = partitionFields)
  def withFileListener(listener: FileListener): HiveSink = copy(fileListener = listener)
  def withFilenameStrategy(filenameStrategy: FilenameStrategy): HiveSink = copy(filenameStrategy = filenameStrategy)
  def withPartitionStrategy(strategy: PartitionStrategy): HiveSink = copy(partitionStrategy = strategy)
  def withMetaData(map: Map[String, String]): HiveSink = copy(metadata = map)
  def withRoundingMode(mode: RoundingMode): HiveSink = copy(roundingMode = mode)
  def withStagingStrategy(strategy: StagingStrategy): HiveSink = copy(stagingStrategy = strategy)
  def withMetastoreSchemaHandler(strategy: MetastoreSchemaHandler): HiveSink = copy(metastoreSchemaHandler = strategy)
  def withAlignmentStrategy(strategy: AlignmentStrategy): HiveSink = copy(alignStrategy = strategy)
  def withOutputSchemaStrategy(strategy: OutputSchemaStrategy): HiveSink = copy(outputSchemaStrategy = strategy)

  /**
    * Add a callback that will be invoked when commit operations are taking place.
    */
  def addCommitCallback(callback: CommitCallback): HiveSink = copy(callbacks = callbacks :+ callback)

  def withKeytabFile(principal: String, keytabPath: java.nio.file.Path): HiveSink = {
    login()
    copy(principal = principal.some, keytabPath = keytabPath.option)
  }

  private def detectDialect(): HiveDialect = {
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

  override def open(schema: StructType): SinkWriter = open(schema, 1).head

  // hive metastore is not good with concurrency in versions < 2.0
  override def open(schema: StructType, n: Int): Seq[SinkWriter] = client.synchronized {
    login()
    upperCaseCheck(schema)

    if (createTable) {
      if (!ops.tableExists(dbName, tableName)) {
        ops.createTable(dbName,
          tableName,
          schema,
          partitionKeys = schema.partitions.map(_.name.toLowerCase) ++ partitionFields,
          dialect = dialect.getOrElse(ParquetHiveDialect()),
          props = Map.empty,
          tableType = TableType.MANAGED_TABLE
        )
      }
    }

    val metastoreSchema = ops.schema(dbName, tableName)
    logger.trace(s"Retrieved metastore schema: $metastoreSchema")

    // use the metastore handler to allow custom logic for handling schema differences between metastore and input
    logger.debug("Invoking evolution strategy to align metastore schema")
    metastoreSchemaHandler.evolve(dbName, tableName, metastoreSchema, schema, client)

    val readDialect = detectDialect
    val partitionKeyNames = ops.partitionKeys(dbName, tableName)

    List.tabulate(n) { k =>
      new HiveSinkWriter(
        schema,
        metastoreSchema,
        dbName,
        tableName,
        partitionKeyNames,
        Some(k.toString),
        readDialect,
        partitionStrategy,
        filenameStrategy,
        stagingStrategy,
        metastoreSchemaHandler,
        alignStrategy,
        outputSchemaStrategy,
        inheritPermissions,
        permission,
        fileListener,
        callbacks,
        roundingMode,
        metadata
      )
    }
  }

  private def upperCaseCheck(schema: StructType): Unit = {
    if (containsUpperCase(schema)) {
      upperCaseAction match {
        case "error" =>
          sys.error("Writing to hive with a schema that contains upper case characters is discouraged because Hive will lowercase the fields, which could lead to subtle case sensitivity bugs. " +
            "It is recommended that you lower case the schema before writing (eg, datastream.withLowerCaseSchema). " +
            "To disable this exception, set eel.hive.sink.upper-case-action=warn or eel.hive.sink.upper-case-action=none")
        case "warn" =>
          logger.warn("Writing to hive with a schema that contains upper case characters is discouraged because Hive will lowercase the fields, which could lead to subtle case sensitivity bugs. " +
            "It is recommended that you lower case the schema before writing (eg, datastream.withLowerCaseSchema). " +
            "To disable this warning, set eel.hive.sink.upper-case-action=none")
        case _ =>
      }
    }
  }
}