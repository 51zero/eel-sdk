package io.eels.component.hive

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import io.eels.component.hdfs.{AclSpec, HdfsSource}
import io.eels.component.hive.partition.PartitionMetaData
import io.eels.datastream.{Subscription, Subscriber}
import io.eels.schema.{Partition, PartitionConstraint, StringType, StructType}
import io.eels.util.HdfsIterator
import io.eels.{FilePattern, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType}
import org.apache.hadoop.security.UserGroupInformation

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode
import scala.util.matching.Regex

case class HiveTable(dbName: String,
                     tableName: String)
                    (implicit fs: FileSystem,
                     conf: Configuration,
                     client: IMetaStoreClient) extends Logging {

  lazy val ops = new HiveOps(client)

  /**
    * Returns all the partitions used by this hive source.
    */
  def partitions(): Seq[Partition] = ops.partitions(dbName, tableName)

  /**
    * Returns all the partitions along with extra meta data per partition, eg location, creation time.
    */
  def partitionMetaData(): Seq[PartitionMetaData] = ops.partitionsMetaData(dbName, tableName)

  def partitionMetaData(partition: Partition): Option[PartitionMetaData] = partitionMetaData().find(_.partition == partition)

  // returns all partition meta data for partitions that match each constraint
  def partitionMetaData(constraints: Seq[PartitionConstraint]): Seq[PartitionMetaData] =
    partitionMetaData().filter { meta => constraints.forall(_.eval(meta.partition)) }

  def hasPartitions: Boolean = partitions().nonEmpty

  /**
    * Returns just the values for the given partition key
    */
  def partitionValues(key: String): Set[String] = partitions.map(_.get(key)).collect {
    case Some(entry) => entry.value
  }.toSet

  def truncatePartition(partition: Partition): Unit = {
    logger.info(s"Truncating partition $partition")
    val meta = partitionMetaData(partition).getOrError(s"Unknown partition $partition")
    new HivePartitionScanner().scan(Seq(meta), Nil).foreach { case (_, files) =>
      logger.debug(s"Deleting partition files ${files.map(_.getPath).mkString(",")}")
      files.map(_.getPath).foreach(fs.delete(_, false))
    }
  }

  def schema: StructType = {
    ops.schema(dbName, tableName)
  }

  def create(schema: StructType,
             partitionFields: Seq[String] = Nil,
             tableType: TableType = TableType.MANAGED_TABLE,
             format: HiveFormat = HiveFormat.Parquet,
             props: Map[String, String] = Map.empty): Unit = {
    if (!ops.tableExists(dbName, tableName)) {
      ops.createTable(dbName,
        tableName,
        schema,
        partitionKeys = schema.partitions.map(_.name.toLowerCase) ++ partitionFields,
        format = format,
        props = props,
        tableType = tableType
      )
    }
  }

  /**
    * Returns a list of all files used by this hive table.
    *
    * @param includePartitionDirs if true then the partition directories will be included
    * @param includeTableDir      if true then the main table directory will be included
    * @return paths of all files and directories
    */
  def paths(includePartitionDirs: Boolean, includeTableDir: Boolean): Seq[Path] = {

    val _location = location

    val partitions = partitionMetaData
    val files = if (partitions.isEmpty) {
      HiveFileScanner(_location, false).map(_.getPath)
    } else {
      partitions.flatMap { partition =>
        val files = FilePattern(s"${partition.location}/*").toPaths()
        if (includePartitionDirs) {
          files :+ partition.location
        } else {
          files
        }
      }
    }

    if (includeTableDir) {
      files :+ _location
    } else {
      files
    }
  }

  /**
    * Returns a list of all files used by this hive table that match the given regex.
    * The full path of the file will be used when matching against the regex.
    *
    * @param includePartitionDirs if true then the partition directories will be included
    * @param includeTableDir      if true then the main table directory will be included
    * @return paths of all files and directories
    */
  def paths(includePartitionDirs: Boolean, includeTableDir: Boolean, regex: Regex): Seq[Path] = {
    paths(includePartitionDirs, includeTableDir).filter { path => regex.pattern.matcher(path.toString).matches }
  }

  /**
    * Returns all the files used by this table. The result is a mapping of partition path to the files contained
    * in that partition.
    */
  def files(): Map[Path, Seq[Path]] = {
    ops.hivePartitions(dbName, tableName).map { p =>
      val location = new Path(p.getSd.getLocation)
      val paths = HdfsIterator.remote(fs.listFiles(location, false)).map(_.getPath).toList
      location -> paths
    }.toMap
  }

  def setPermissions(permission: FsPermission,
                     includePartitionDirs: Boolean = false,
                     includeTableDir: Boolean = false): Unit = {
    paths(includePartitionDirs, includeTableDir).foreach(fs.setPermission(_, permission))
  }

  def showDdl(ifNotExists: Boolean = true): String = {
    val _spec = spec()
    val partitions = ops.partitionKeys(dbName, tableName)
    HiveDDL.showDDL(
      tableName,
      schema.fields,
      tableType = _spec.tableType,
      location = _spec.location.some,
      serde = _spec.serde,
      partitions = partitions.map(PartitionColumn(_, StringType)),
      outputFormat = _spec.outputFormat,
      inputFormat = _spec.inputFormat,
      ifNotExists = ifNotExists)
  }

  /**
    * Sets the acl for all files of this hive source.
    * Even if the files are not located inside the table directory, this function will find them
    * and correctly update the spec.
    *
    * @param acl the acl values to set
    */
  def setAcl(acl: AclSpec,
             includePartitionDirs: Boolean = false,
             includeTableDir: Boolean = false): Unit = {
    paths(includePartitionDirs, includeTableDir).foreach { path =>
      HdfsSource(path).setAcl(acl)
    }
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
  def spec(): TableSpec = client.synchronized {
    val table = client.getTable(dbName, tableName)
    val tableType = TableType.values().find(_.name.toLowerCase == table.getTableType.toLowerCase)
      .getOrError("Hive table type is not supported by this version of hive")
    val params = table.getParameters.asScala.toMap ++ table.getSd.getParameters.asScala.toMap
    TableSpec(
      tableName,
      tableType,
      table.getSd.getLocation,
      table.getSd.getCols.asScala,
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

  def dialect = client.synchronized {
    io.eels.component.hive.HiveDialect(client.getTable(dbName, tableName))
  }

  // todo use dialect to return correct stats
  def stats(): HiveStats = new ParquetHiveStats(dbName, tableName, this)

  // will compact all the files in each partitions into a single file
  def compact(finalFilename: String = "eel_compacted_" + System.nanoTime): Unit = {
    val _schema = schema
    val _dialect = dialect
    HiveTableFilesFn(dbName, tableName, location, Nil).filter(_._2.nonEmpty).foreach { case (partition, files) =>
      logger.info(s"Starting compact for $partition")
      val queue = new LinkedBlockingQueue[Seq[Row]]
      val done = new AtomicInteger(0)
      files.foreach { file =>
        _dialect.input(file.getPath, _schema, _schema, None).subscribe(new Subscriber[Seq[Row]] {
          override def next(t: Seq[Row]): Unit = queue.put(t)
          override def completed(): Unit = if (done.incrementAndGet == files.size) {
            queue.put(Row.Sentinel)
          }
          override def error(t: Throwable): Unit = {
            logger.error(s"Error compacting $partition", t)
            queue.put(Row.Sentinel)
          }
          override def subscribed(c: Subscription): Unit = ()
        })
      }
      val output = _dialect.output(_schema, new Path(files.head.getPath.getParent, finalFilename), None, RoundingMode.UNNECESSARY, Map.empty)
      BlockingQueueConcurrentIterator(queue, Row.Sentinel).foreach { rows =>
        rows.foreach(output.write)
      }
      output.close()
      logger.info(s"Finished compact for $partition")
    }
  }

  // returns the location of this table as a hadoop Path
  def location(): Path = new Path(spec().location)

  def deletePartition(partition: Partition, deleteData: Boolean): Unit = client.synchronized {
    logger.debug(s"Deleting partition ${partition.unquoted}")
    client.dropPartition(dbName, tableName, partition.values.asJava, deleteData)
  }

  def drop(deleteData: Boolean = true): Unit = client.synchronized {
    logger.debug(s"Dropping table $dbName:$tableName")
    client.dropTable(dbName, tableName, deleteData, true)
  }

  def truncate(removePartitions: Boolean): Unit = client.synchronized {
    logger.debug(s"Truncating table $dbName:$tableName")
    if (removePartitions)
      new HiveOps(client).partitions(dbName, tableName).foreach(deletePartition(_, true))
    else {
      files().values.foreach(_.foreach(path => fs.delete(path, false)))
    }
  }

  def login(principal: String, keytabPath: java.nio.file.Path): Unit = {
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath.toString)
  }

  def toHdfsSource = HdfsSource(FilePattern(location.toString + "/*"))

  def source = HiveSource(dbName, tableName)
  def sink = HiveSink(dbName, tableName)
}
