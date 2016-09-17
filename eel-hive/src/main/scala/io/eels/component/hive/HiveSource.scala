package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.component.hdfs.HdfsSource
import io.eels.schema.{PartitionConstraint, PartitionEquals, Schema}
import io.eels.{Part, Source}
import io.eels.component.parquet.{ParquetLogMute, Predicate}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType}

import scala.collection.JavaConverters._

/**
 * @param constraints optional constraits on the partition data to narrow which partitions are read
 * @param projection sets which fields are required by the caller.
 * @param predicate optional predicate which will filter rows at the read level
 *
 */
case class HiveSource(dbName: String,
                      tableName: String,
                      constraints: List[PartitionConstraint] = Nil,
                      projection: List[String] = Nil,
                      predicate: Option[Predicate] = None)
                     (implicit fs: FileSystem,
                      client: IMetaStoreClient) extends Source with Logging with Using {
    ParquetLogMute()

  implicit val conf = fs.getConf
  val ops = new HiveOps(client)

  def select(first: String, rest: String*): HiveSource = withProjection(first +: rest)
  def withProjection(first: String, rest: String*): HiveSource = withProjection(first +: rest)
  def withProjection(columns: Seq[String]): HiveSource = {
    require(columns.nonEmpty)
    copy(projection = columns.toList)
  }

  // returns the permission of the table location path
  def tablePermission(): FsPermission = {
    val location = ops.location(dbName, tableName)
    fs.getFileStatus(new Path(location)).getPermission
  }

  /**
    * Returns a TableSpec which contains details of the underlying table.
    * Similar to the Table class in the Hive API but using scala friendly types.
    */
  def spec(): TableSpec = {
    val table = client.getTable(dbName, tableName)
    val tableType = TableType.values().find(_.name.toLowerCase == table.getTableType.toLowerCase)
      .getOrElse(sys.error("Hive table type is not supported by this version of hive"))
    val params = table.getParameters.asScala.toMap
    TableSpec(
      tableName,
      tableType,
      table.getSd.getLocation,
      table.getSd.getNumBuckets,
      table.getSd.getBucketCols.asScala.toList,
      params,
      table.getSd.getInputFormat,
      table.getSd.getOutputFormat,
      table.getSd.getSerdeInfo.getName,
      table.getRetention,
      table.getCreateTime,
      table.getLastAccessTime,
      table.getOwner
    )
  }

  def withPredicate(predicate: Predicate): HiveSource = copy(predicate = Some(predicate))

  def withPartitionConstraint(name: String, value: String): HiveSource = withPartitionConstraint(PartitionEquals(name, value))

  def withPartitionConstraint(expr: PartitionConstraint): HiveSource = {
    copy(constraints = constraints :+ expr)
  }

  /**
   * The returned schema should take into account:
   *
   * 1) Any projection. If a projection is set, then it should return the schema in the same order
   * as the projection. If no projection is set then the schema should be driven from the hive metastore.
   *
   * 2) Any partitions set. These should be included in the schema columns.
   */
  override def schema(): Schema = {

    // if no field names were specified, then we will return the schema as is from the hive database,
    // otherwise we will keep only the requested fields
    val schema = if (projection.isEmpty) metastoreSchema
    else {
      // remember hive is always lower case, so when comparing requested field names with
      // hive fields we need to use lower case everything. And we need to return the schema
      // in the same order as the requested projection
      val columns = projection.map { fieldName =>
        val field = metastoreSchema.fields.find { it => it.name.equals(fieldName, true) }
        field.getOrElse(sys.error(s"Requested field $fieldName does not exist in the hive schema"))
      }
      Schema(columns)
    }

    schema
  }

  // returns the full underlying schema from the metastore including partition partitionKeys
  val metastoreSchema: Schema = ops.schema(dbName, tableName)

  //def  spec(): HiveSpec = HiveSpecFn.toHiveSpec(dbName, tableName)

  def isPartitionOnlyProjection(): Boolean = {
    val partitionKeyNames = HiveTable(dbName, tableName).partitionKeys().map(_.field.name)
    projection.nonEmpty && projection.map { it => it.toLowerCase() }.forall { it => partitionKeyNames.contains(it) }
  }

  override def parts(): List[Part] = {

    val table = client.getTable(dbName, tableName)
    val dialect = io.eels.component.hive.HiveDialect(table)
    val partitionKeys = HiveTable(dbName, tableName).partitionKeys()

    // all field names from the underlying hive schema
    val fieldNames = metastoreSchema.fields.map(_.name)

    // if we requested only partition columns, then we can get this information by scanning the file system
    // to see which partitions have been created. Then the presence of files in a partition location means that
    // that partition must have data.
    if (isPartitionOnlyProjection()) {
      logger.info("Requested projection contains only partitions; reading directly from metastore")
      // we pass in the schema so we can order the results to keep them aligned with the given projection
      List(new HivePartitionPart(dbName, tableName, fieldNames, Nil, metastoreSchema, predicate, dialect))
    } else {
      val files = HiveFilesFn(table, partitionKeys.map(_.field.name), constraints)
      logger.debug(s"Found ${files.size} visible hive files from all locations for $dbName:$tableName")

      // for each seperate hive file part we must pass in the metastore schema
      files.map { case (file, spec) =>
        new HiveFilePart(dialect, file, metastoreSchema, schema(), predicate, spec.parts.toList)
      }
    }
  }
}