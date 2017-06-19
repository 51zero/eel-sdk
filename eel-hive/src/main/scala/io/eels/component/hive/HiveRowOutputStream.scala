package io.eels.component.hive

import java.util.UUID
import java.util.concurrent._

import com.sksamuel.exts.Logging
import com.typesafe.config.ConfigFactory
import io.eels.schema.StructType
import io.eels.util.RowNormalizerFn
import io.eels.{Row, RowOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.collection.concurrent.TrieMap

class HiveRowOutputStream(sourceSchema: StructType,
                          metastoreSchema: StructType,
                          dbName: String,
                          tableName: String,
                          // a discriminator for the file names, needed when we are writing to the same table
                          // with multiple threads
                          discriminator: Option[String],
                          dialect: HiveDialect,
                          dynamicPartitioning: Boolean,
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
  private val lock = new AnyRef()

  // these will be in lower case
  private val partitionKeyNames = hiveOps.partitionKeyNames(dbName, tableName)

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
  private val writers = TrieMap.empty[String, HiveWriter]

  // this contains all the partitions we've checked.
  private val createdPartitions = new ConcurrentSkipListSet[String]

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

  def getOrCreateHiveWriter(row: Row): HiveWriter = {

    // we need a a writer per partition (as each partition is written to a different directory)
    val parts = PartitionPartsFn.rowPartitionParts(row, partitionKeyNames)
    val partPath = hiveOps.partitionPathString(parts, tablePath)

    // we cache the writer so that we don't keep opening and closing loads of writers
    writers.getOrElseUpdate(partPath, {

      // if dynamic partition is enabled then we will create any partitions and
      // update the hive metastore
      if (dynamicPartitioning) {
        if (parts.nonEmpty) {
          if (!createdPartitions.contains(partPath.toString())) {
            hiveOps.createPartitionIfNotExists(dbName, tableName, parts)
            createdPartitions.add(partPath.toString())
          }
        }
      } else if (!hiveOps.partitionExists(dbName, tableName, parts)) {
        sys.error(s"Partition $partPath does not exist and dynamicPartitioning = false")
      }

      // ensure the part path is created, with permissions from parent
      if (inheritPermissions.getOrElse(inheritPermissionsDefault)) {
        val parent = Iterator.iterate(new Path(partPath))(_.getParent).dropWhile(false == fs.exists(_)).take(1).toList.head
        val permission = fs.getFileStatus(parent).getPermission
        Iterator.iterate(new Path(partPath))(_.getParent).takeWhile(false == fs.exists(_)).foreach { path =>
          fs.create(path, false)
          fs.setPermission(path, permission)
        }
      }

      val filename = "eel_" + System.nanoTime() + discriminator.map("_" + _.stripPrefix("_")).getOrElse("")
      val filePath = if (writeToTempDirectory) {
        val temp = new Path(partPath, ".eeltemp_" + UUID.randomUUID.toString)
        new Path(temp, filename)
      } else {
        new Path(partPath, filename)
      }
      logger.debug(s"Creating hive writer for $filePath")
      fileListener.onFileCreated(filePath)

      dialect.writer(fileSchema, filePath, permission, metadata)
    })
  }
}