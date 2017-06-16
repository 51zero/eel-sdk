package io.eels.component.hive

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, _}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.collection.BlockingQueueConcurrentIterator
import com.typesafe.config.ConfigFactory
import io.eels.schema.StructType
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
                          ioThreads: Int,
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
  private val normalizer = new RowNormalizerFn(fileSchema)

  // Since the data can come in unordered, we need to keep open a stream per partition path otherwise we'd
  // be opening and closing streams frequently.
  // We also can't share a writer amongst threads for obvious reasons otherwise we'd just have a single
  // writer being used for all data. So the solution is a hive writer per thread per partition, each
  // writing to their own files.
  private val writers = new TrieMap[String, (Path, HiveWriter)]

  // this contains all the partitions we've checked.
  // No need for multiple threads to keep hitting the meta store to check on the same partition paths
  private val createdPartitions = new ConcurrentSkipListSet[String]

  // we buffer incoming data into this queue, so that slow writing doesn't unncessarily
  // block threads feeding this sink
  private val buffer = new LinkedBlockingQueue[Row](bufferSize)

  // the io threads will run in their own threads inside this executor
  private val writerPool = Executors.newFixedThreadPool(ioThreads)

  logger.debug(s"HiveSinkWriter created; dynamicPartitioning=$dynamicPartitioning; ioThreads=$ioThreads")

  import com.sksamuel.exts.concurrent.ExecutorImplicits._

  private val running = new AtomicBoolean(true)
  private var throwable: Throwable = null

  logger.debug(s"Creating $ioThreads hive writers")
  for (k <- 0 until ioThreads) {
    writerPool.submit {
      try {
        BlockingQueueConcurrentIterator(buffer, Row.Sentinel).takeWhile(_ => running.get).foreach { row =>
          val writer = getOrCreateHiveWriter(row, k)._2
          // need to strip out any partition information from the written data and possibly pad
          writer.write(normalizer(row))
        }
      } catch {
        case NonFatal(e) =>
          logger.error("Error during write; aborting all writing threads", e)
          running.set(false)
          throwable = e
      }
    }
  }
  writerPool.shutdown()

  case class WriteStatus(path: Path, fileSizeInBytes: Long, records: Int)

  // returns a Map consisting of each path written, the size of the file, and the number of records in that file
  def writeStats(): Seq[WriteStatus] = {
    writers.map { case (_, ph) => WriteStatus(ph._1, fs.getFileStatus(ph._1).getLen, ph._2.records) }
  }.toVector

  override def write(row: Row): Unit = {
    if (throwable != null)
      throw throwable
    buffer.put(row)
  }

  override def close(): Unit = {
    logger.debug("Closing hive writer once buffer is empty...")

    buffer.put(Row.Sentinel)
    writerPool.awaitTermination(100, TimeUnit.DAYS)
    writers.values.foreach(_._2.close)
    logger.debug(s"All hive writer threads have completed; usedTempDir=$writeToTempDirectory")

    if (writeToTempDirectory) {
      logger.debug("Moving files from temp dir to public")
      // move table/.temp/file to table/file
      writers.values.foreach { case (path, _) => fs.rename(path, new Path(path.getParent.getParent, path.getName)) }
      logger.debug("Deleting temp dirs")
      writers.values.foreach { case (path, _) =>
        fs.delete(path.getParent, true)
      }
    }
  }

  def getOrCreateHiveWriter(row: Row, writerId: Int): (Path, HiveWriter) = {

    // we need a hive writer per thread (to different files of course), and a writer
    // per partition (as each partition is written to a different directory)
    val parts = PartitionPartsFn.rowPartitionParts(row, partitionKeyNames)
    val partPath = hiveOps.partitionPathString(parts, tablePath)
    writers.getOrElseUpdate(partPath + writerId, {

      // if dynamic partition is enabled then we will try to update
      // the hive metastore with the new partition information
      if (dynamicPartitioning) {
        if (parts.nonEmpty) {
          // we need to synchronize this, as its quite likely that when ioThreads>1 we have >1 thread
          // trying to createReader a partition at the same time. This is virtually guaranteed to happen if
          // the data is in any way sorted
          if (!createdPartitions.contains(partPath.toString())) {
            lock.synchronized {
              hiveOps.createPartitionIfNotExists(dbName, tableName, parts)
              createdPartitions.add(partPath.toString())
            }
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

      val filename = "eel_" + System.nanoTime() + "_" + writerId
      val filePath = if (writeToTempDirectory) {
        val temp = new Path(partPath, ".eeltemp_" + UUID.randomUUID.toString)
        new Path(temp, filename)
      } else {
        new Path(partPath, filename)
      }
      logger.debug(s"Creating hive writer for $filePath")
      fileListener.onFileCreated(filePath)
      filePath -> dialect.writer(fileSchema, filePath, permission, metadata)
    })
  }
}