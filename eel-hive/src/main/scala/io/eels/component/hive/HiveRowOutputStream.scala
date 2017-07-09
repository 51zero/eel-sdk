package io.eels.component.hive

import java.util.concurrent._

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.component.hive.partition.{PartitionPathStrategy, RowPartitionFn}
import io.eels.schema.{Partition, StructType}
import io.eels.util.{HdfsMkdir, RowNormalizerFn}
import io.eels.{Row, RowOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal

class HiveRowOutputStream(sourceSchema: StructType,
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
                          bufferSize: Int,
                          inheritPermissions: Option[Boolean],
                          permission: Option[FsPermission],
                          fileListener: FileListener,
                          metadata: Map[String, String])
                         (implicit fs: FileSystem,
                          conf: Configuration,
                          client: IMetaStoreClient) extends RowOutputStream with Logging {

  private val config = ConfigFactory.load()
  private val sinkConfig = HiveSinkConfig()
  private val writeToTempDirectory = config.getBoolean("eel.hive.sink.writeToTempFiles")
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
    if (sinkConfig.includePartitionsInData || partitionKeyNames.isEmpty)
      metastoreSchema
    else
      partitionKeyNames.foldLeft(metastoreSchema) { (schema, name) =>
        schema.removeField(name, caseSensitive = false)
      }
  }

  // the normalizer takes care of making sure the row is aligned with the file schema
  private val normalizer = new RowNormalizerFn(fileSchema, ConfigFactory.load().getBoolean("eel.hive.sink.pad-with-null"))

  // Since the data can come in unordered, we want to keep the streams for each partition open
  // otherwise we would be opening and closing streams frequently.
  private val writers = TrieMap.empty[Path, HiveWriter]

  // this contains all the partitions we've checked.
  private val extantPartitions = new ConcurrentSkipListSet[Path]

  logger.debug(s"HiveSinkWriter created; dynamicPartitioning=$dynamicPartitioning")

  case class WriteStatus(path: Path, fileSizeInBytes: Long, records: Int)

  // returns a Map consisting of each path written, the size of the file, and the number of records in that file
  def writeStats(): Seq[WriteStatus] = {
    writers.values.map { writer => WriteStatus(writer.path, fs.getFileStatus(writer.path).getLen, writer.records) }
  }.toVector

  override def write(row: Row): Unit = {
    val writer = getOrCreateHiveWriter(row)
    // need to strip out any partition information from the written data and possibly pad
    writer.write(normalizer(row))
  }

  override def close(): Unit = {
    logger.info("Closing hive output stream")
    if (writeToTempDirectory) {
      logger.info("Moving files from temp dir to public")

      // move table/.temp/file to table/file
      writers.values.foreach { writer =>
        fs.rename(writer.path, new Path(writer.path.getParent.getParent, writer.path.getName))
      }

      logger.debug("Deleting temp dirs")
      writers.values.foreach { writer =>
        fs.delete(writer.path, true)
      }
    }
    logger.debug(s"Closing ${writers.size} hive writers")
    writers.values.foreach(_.close)
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

  private def createPartitionWriter(partition: Partition, partitionPath: Path): HiveWriter = {
    ensurePartitionExists(partition, partitionPath)
    // ensure the partition path is created, with permissions from parent if applicable
    HdfsMkdir(partitionPath, inheritPermissions.getOrElse(inheritPermissionsDefault))
    createWriter(partitionPath)
  }

  private def createWriter(location: Path): HiveWriter = try {
    logger.debug(s"Creating new HiveWriter for location $location")

    val filePath = outputPath(location)
    logger.debug(s"HiveWriter will write to file $filePath")
    fileListener.onFileCreated(filePath)

    dialect.writer(fileSchema, filePath, permission, metadata)
  } catch {
    case NonFatal(e) =>
      logger.error(s"Error getting or creating the hive writer for $location", e)
      throw e
  }

  private def outputPath(partitionPath: Path): Path = {
    val filename = filenameStrategy.filename(discriminator)
    if (writeToTempDirectory) {
      val temp = new Path(partitionPath, filenameStrategy.tempdir)
      new Path(temp, filename)
    } else {
      new Path(partitionPath, filename)
    }
  }

  def getOrCreateHiveWriter(row: Row): HiveWriter = {

    // we need a a writer per partition (as each partition is written to a different directory)
    // if we don't have partitions then we only need a writer for the table
    if (partitionKeyNames.isEmpty) {
      writers.getOrElseUpdate(tablePath, createWriter(tablePath))
    } else {

      val partition = RowPartitionFn(row, partitionKeyNames)
      val partitionPath = new Path(tablePath, partitionPathStrategy.name(partition))

      // we cache the writer so that we don't keep opening and closing loads of writers
      writers.getOrElseUpdate(partitionPath, createPartitionWriter(partition, partitionPath))
    }
  }
}