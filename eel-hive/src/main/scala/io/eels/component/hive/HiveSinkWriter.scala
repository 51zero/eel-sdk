package io.eels.component.hive

import java.util.concurrent._

import com.sksamuel.exts.Logging
import com.sksamuel.exts.OptionImplicits._
import com.typesafe.config.ConfigFactory
import io.eels.component.hive.partition.{PartitionPathStrategy, RowPartitionFn}
import io.eels.schema.{Partition, StructType}
import io.eels.util.HdfsMkdir
import io.eels.{Row, SinkWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.collection.concurrent.TrieMap
import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode.RoundingMode
import scala.util.control.NonFatal

class HiveSinkWriter(sourceSchema: StructType,
                     metastoreSchema: StructType,
                     dbName: String,
                     tableName: String,
                     // a discriminator for the file names, needed when we are writing to the same table
                     // with multiple threads
                     discriminator: Option[String],
                     dialect: HiveDialect,
                     dynamicPartitioning: Boolean,
                     partitionPathStrategy: PartitionPathStrategy,
                     filenameStrategy: FilenameStrategy,
                     stagingStrategy: StagingStrategy,
                     evolutionStrategy: EvolutionStrategy = DefaultEvolutionStrategy,
                     bufferSize: Int,
                     inheritPermissions: Option[Boolean],
                     permission: Option[FsPermission],
                     fileListener: FileListener,
                     callbacks: Seq[CommitCallback],
                     roundingMode: RoundingMode = RoundingMode.UNNECESSARY,
                     metadata: Map[String, String])
                    (implicit fs: FileSystem,
                     conf: Configuration,
                     client: IMetaStoreClient) extends SinkWriter with Logging {
  logger.debug(s"HiveSinkWriter created; dynamicPartitioning=$dynamicPartitioning")

  private val config = ConfigFactory.load()
  private val includePartitionsInData = ConfigFactory.load.getBoolean("eel.hive.sink.include-partitions-in-data")
  private val inheritPermissionsDefault = config.getBoolean("eel.hive.sink.inheritPermissions")

  private val hiveOps = new HiveOps(client)
  private val tablePath = hiveOps.tablePath(dbName, tableName)
  require(tablePath != null, "Table path returned as null")

  // these will be in lower case
  private val partitionKeyNames = hiveOps.partitionKeys(dbName, tableName)

  // the file schema is the metastore schema with the partition columns removed. This is because the
  // partition columns are not written to the file (they are taken from the partition itself)
  // this can be overriden with the includePartitionsInData option in which case the partitions will
  // be kept in the file
  private val fileSchema = {
    if (includePartitionsInData || partitionKeyNames.isEmpty)
      metastoreSchema
    else
      partitionKeyNames.foldLeft(metastoreSchema) { (schema, name) =>
        schema.removeField(name, caseSensitive = false)
      }
  }

  // Since the data can come in unordered, we want to keep the streams for each partition open
  // otherwise we would be opening and closing streams frequently.
  private val streams = TrieMap.empty[Path, HiveOutputStream]

  // this contains all the partitions we've checked.
  private val extantPartitions = new ConcurrentSkipListSet[Path]

  case class WriteStatus(path: Path, fileSizeInBytes: Long, records: Int)

  // returns a Map consisting of each path written, the size of the file, and the number of records in that file
  def writeStats(): Seq[WriteStatus] = {
    streams.values.map { writer => WriteStatus(writer.path, fs.getFileStatus(writer.path).getLen, writer.records) }
  }.toVector

  override def write(row: Row): Unit = {
    val stream = getOrCreateHiveWriter(row)
    // need to ensure the row is compatible with the metastore
    stream.write(evolutionStrategy.align(row, metastoreSchema))
  }

  override def close(): Unit = {
    logger.info("Closing hive sink writer")

    logger.debug(s"Closing ${streams.size} hive output stream(s)")
    streams.values.foreach(_.close)

    if (stagingStrategy.staging) {
      logger.info("Staging was enabled, committing staging files to public")

      // move files from the staging area into the public area
      streams.foreach { case (location, writer) =>
        val stagingPath = writer.path
        val finalPath = new Path(location, stagingPath.getName)
        logger.debug(s"Committing file $stagingPath => $finalPath")
        fs.rename(stagingPath, finalPath)
        callbacks.foreach(_.onCommit(stagingPath, finalPath))
      }

      streams.values.foreach { writer =>
        val stagingDir = writer.path.getParent
        logger.debug(s"Deleting staging directory $stagingDir")
        fs.delete(stagingDir, true)
      }

      logger.info("Commit completed")
      callbacks.foreach(_.onCommitComplete)
    }
  }

  private def ensurePartitionExists(partition: Partition, path: Path): Unit = {
    // if dynamic partitioning is enabled then we will update the hive metastore with new partitions
    if (partitionKeyNames.nonEmpty && dynamicPartitioning) {
      if (partition.entries.nonEmpty) {
        if (!extantPartitions.contains(path)) {
          hiveOps.createPartitionIfNotExists(dbName, tableName, partition, partitionPathStrategy)
          extantPartitions.add(path)
        }
      }
    } else if (!hiveOps.partitionExists(dbName, tableName, partition)) {
      sys.error(s"Partition $path does not exist and dynamicPartitioning = false")
    }
  }

  private def createPartitionWriter(partition: Partition, partitionPath: Path): HiveOutputStream = {
    ensurePartitionExists(partition, partitionPath)
    // ensure the partition path is created, with permissions from parent if applicable
    HdfsMkdir(partitionPath, inheritPermissions.getOrElse(inheritPermissionsDefault))
    createWriter(partitionPath)
  }

  private def createWriter(location: Path): HiveOutputStream = try {
    logger.debug(s"Creating new hive output stream for location $location")

    val filePath = outputPath(location)
    logger.debug(s"Hive output stream will write to file $filePath")
    assert(filePath.isAbsolute, s"Output stream path must be absolute (was $filePath)")
    fileListener.onOutputFile(filePath)

    dialect.output(fileSchema, filePath, permission, roundingMode, metadata)

  } catch {
    case NonFatal(e) =>
      logger.error(s"Error getting or creating the hive output stream for $location", e)
      throw e
  }

  private def outputPath(partitionPath: Path): Path = {
    val filename = filenameStrategy.filename + discriminator.getOrElse("")
    if (stagingStrategy.staging) {
      val stagingDirectory = stagingStrategy.stagingDirectory(partitionPath)
        .getOrError("Staging strategy returned None, but staging was enabled. This is a bug in the staging strategy.")
      val temp = new Path(stagingDirectory, filename)
      new Path(temp, filename)
    } else {
      new Path(partitionPath, filename)
    }
  }

  // if partitioning is used, inspects the row to see which partition it should live in
  // and returns an output stream for that partition
  // if partitioning is not used then will return the same table stream for all rows
  private def getOrCreateHiveWriter(row: Row): HiveOutputStream = {

    // we need a a writer per partition (as each partition is written to a different directory)
    // if we don't have partitions then we only need a writer for the table
    if (partitionKeyNames.isEmpty) {
      streams.getOrElseUpdate(tablePath, createWriter(tablePath))
    } else {

      val partition = RowPartitionFn(row, partitionKeyNames)
      val partitionPath = new Path(tablePath, partitionPathStrategy.name(partition))

      // we cache the writer so that we don't keep opening and closing loads of writers
      streams.getOrElseUpdate(partitionPath, createPartitionWriter(partition, partitionPath))
    }
  }
}