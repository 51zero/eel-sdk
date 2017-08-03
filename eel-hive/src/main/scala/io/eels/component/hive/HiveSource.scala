package io.eels.component.hive

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.io.Using
import io.eels.component.parquet.util.ParquetLogMute
import io.eels.datastream.Publisher
import io.eels.schema.{PartitionConstraint, StructType}
import io.eels.{Chunk, Predicate, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.security.UserGroupInformation

/**
  * @param constraints optional constraits on the partition data to narrow which partitions are read
  * @param projection  sets which fields are required by the caller.
  * @param predicate   optional predicate which will filter rows at the read level
  *
  */
case class HiveSource(dbName: String,
                      tableName: String,
                      projection: List[String] = Nil,
                      predicate: Option[Predicate] = None,
                      partitionConstraints: Seq[PartitionConstraint] = Nil,
                      principal: Option[String] = None,
                      keytabPath: Option[java.nio.file.Path] = None)
                     (implicit fs: FileSystem,
                      client: IMetaStoreClient) extends Source with Logging with Using {
  ParquetLogMute()

  implicit private val conf: Configuration = fs.getConf
  private val ops = new HiveOps(client)

  def withProjection(first: String, rest: String*): HiveSource = withProjection(first +: rest)
  def withProjection(columns: Seq[String]): HiveSource = {
    require(columns.nonEmpty)
    copy(projection = columns.toList)
  }

  def withPredicate(predicate: Predicate): HiveSource = copy(predicate = predicate.some)

  def withPartitionConstraints(constraints: Seq[PartitionConstraint]): HiveSource = copy(partitionConstraints = constraints)
  def addPartitionConstraint(constraint: PartitionConstraint): HiveSource =
    copy(partitionConstraints = partitionConstraints :+ constraint)

  def withKeytabFile(principal: String, keytabPath: java.nio.file.Path): HiveSource = {
    login()
    copy(principal = principal.some, keytabPath = keytabPath.option)
  }

  private def login(): Unit = {
    for (user <- principal; path <- keytabPath) {
      UserGroupInformation.loginUserFromKeytab(user, path.toString)
    }
  }

  def table = HiveTable(dbName, tableName)

  /**
    * The returned schema should take into account:
    *
    * 1) Any projection. If a projection is set, then it should return the schema in the same order
    * as the projection. If no projection is set then the schema should be driven from the hive metastore.
    *
    * If the projection requests a field that does not exist, then this method will throw an exception.
    *
    * 2) Any partitions set. These should be included in the schema columns.
    */
  override def schema: StructType = {
    login()
    // if no field names were specified, then we will return the schema as is from the hive database,
    // otherwise we will keep only the requested fields
    val schema = if (projection.isEmpty) metastoreSchema
    else {
      // remember hive is always lower case, so when comparing requested field names with
      // hive fields we need to use lower case everything. And we need to return the schema
      // in the same order as the requested projection
      val columns = projection.map { fieldName =>
        metastoreSchema.fields
          .find(_.name == fieldName.toLowerCase)
          .getOrElse(sys.error(s"Requested field $fieldName does not exist in the hive schema"))
      }
      StructType(columns)
    }

    schema
  }

  // returns the full underlying schema from the metastore including partition partitionKeys
  private lazy val metastoreSchema: StructType = {
    login()
    ops.schema(dbName, tableName)
  }

  /**
    * Returns true if the currently set projection is for partition only fields.
    */
  def isPartitionOnlyProjection: Boolean = {
    val partitionKeyNames = ops.partitionKeys(dbName, tableName)
    projection.nonEmpty && projection.map { it => it.toLowerCase() }.forall { it => partitionKeyNames.contains(it) }
  }

  override def parts(): Seq[Publisher[Chunk]] = {
    login()

    val dialect = table.dialect
    val partitionKeys = ops.partitionKeys(dbName, tableName)

    // a predicate cannot operate on partitions, as a predicate is pushed down into the files
    // but partition data is not always written out to the file
    if (predicate.map(_.fields).getOrElse(Nil).exists(partitionKeys.contains))
      sys.error("A predicate cannot operate on partition fields; use a partition constraint")

    // if we requested only partition columns, then we can get this information by scanning the metatstore
    // to see which partitions have been created.
    if (isPartitionOnlyProjection) {
      logger.info("Requested projection only uses partitions; reading directly from metastore")
      // we pass in the schema so we can order the results to keep them aligned with the given projection
      List(new HivePartitionPublisher(dbName, tableName, schema, partitionKeys, dialect))
    } else {

      val filesandpartitions = HiveTableFilesFn(dbName, tableName, table.location(), partitionConstraints)
      logger.debug(s"Found ${filesandpartitions.size} visible hive files from all locations for $dbName:$tableName")

      // for each seperate hive file part we must pass in the metastore schema
      filesandpartitions.flatMap { case (partition, files) =>
        files.map { file =>
          new HiveFilePublisher(dialect, file, metastoreSchema, schema, predicate, partition)
        }
      }.toSeq
    }
  }
}