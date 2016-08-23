package io.eels.component.hive

import io.eels.Row
import io.eels.SinkWriter
import io.eels.schema.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import java.util.concurrent.{LinkedBlockingQueue, _}

import com.sksamuel.exts.Logging

import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal

class HiveSinkWriter(sourceSchema: Schema,
                     metastoreSchema: Schema,
                     val dbName: String,
                     val tableName: String,
                     ioThreads: Int,
                     val dialect: HiveDialect,
                     val dynamicPartitioning: Boolean,
                     includePartitionsInData: Boolean,
                     bufferSize: Int)
                    (implicit fs: FileSystem,
                     client: IMetaStoreClient) extends SinkWriter with Logging {

  val ops = new HiveOps(client)
  val tablePath = ops.tablePath(dbName, tableName)
  val lock = new AnyRef()

  // these will be in lower case
  val partitionKeyNames = ops.partitionKeyNames(dbName, tableName)

  // the file schema is the metastore schema with the partition columns removed. This is because the
  // partition columns are not written to the file (they are taken from the partition itself)
  // this can be overriden with the includePartitionsInData option
  val fileSchema = if (includePartitionsInData || partitionKeyNames.isEmpty) metastoreSchema
  else partitionKeyNames.foldLeft(metastoreSchema) { (schema, name) =>
    schema.removeField(name, caseSensitive = false)
  }

  // these are the indexes of the cells to skip in each row because they are partition values
  // we build up the ones to skip, then we can make an array that contains only the ones to keep
  // this array is used to iterate over using the indexes to pick out the values from the row quickly
  val indexesToSkip = if (includePartitionsInData) Nil else partitionKeyNames.map(sourceSchema.indexOf)
  val indexesToWrite = List.range(0, sourceSchema.size).filterNot(indexesToSkip.contains)
  assert(indexesToWrite.nonEmpty, "Cannot write frame where all fields are partitioned")

  // Since the data can come in unordered, we need to keep open a stream per partition path otherwise we'd
  // be opening and closing streams frequently.
  // We also can't share a writer amongst threads for obvious reasons otherwise we'd just have a single
  // writer being used for all data. So the solution is a hive writer per thread per partition, each
  // writing to their own files.
  val writers = new TrieMap[String, HiveWriter]

  // this contains all the partitions we've checked.
  // No need for multiple threads to keep hitting the meta store to check on the same partition paths
  val createdPartitions = new ConcurrentSkipListSet[String]

  // we buffer incoming data into this queue, so that slow writing doesn't unncessarily
  // block threads feeding this sink
  val buffer = new LinkedBlockingQueue[Row](bufferSize)
  val latch = new CountDownLatch(1)

  val writerPool = Executors.newFixedThreadPool(ioThreads)

  val base = System.nanoTime()
  logger.debug(s"HiveSinkWriter created; timestamp=$base; dynamicPartitioning=$dynamicPartitioning; ioThreads=$ioThreads; includePartitionsInData=$includePartitionsInData")

  import com.sksamuel.exts.concurrent.ExecutorImplicits._

  writerPool.submit {
    // this won't work for multiple threads, as the first thread will read the pill
    // todo make this multithreaded
    try {
      Iterator.continually(buffer.take).takeWhile(_ != Row.PoisonPill).foreach { row =>
        // todo make this multithreaded by using some unique id
        val writer = getOrCreateHiveWriter(row, 1L)
        // need to strip out any partition information from the written data
        // keeping this as a list as I want it ordered and no need to waste cycles on an ordered map
        val rowToWrite = if (indexesToSkip.isEmpty) {
          row
        } else {
          val values = for (k <- indexesToWrite) yield {
            row.get(k)
          }
          Row(fileSchema, values.toVector)
        }
        writer.write(rowToWrite)
      }
    } catch {
      case NonFatal(e) =>
        logger.error("Could not perform write", e)
    }
  }
  writerPool.shutdown()

  override def write(row: Row): Unit = buffer.put(row)

  override def close(): Unit = {
    logger.debug("Request to close hive sink writer")
    buffer.put(Row.PoisonPill)
    writerPool.awaitTermination(1, TimeUnit.DAYS)
    logger.debug("All writers have finished writing rows; closing writers to ensure flush")
    writers.values.foreach(_.close)
  }

  def getOrCreateHiveWriter(row: Row, writerId: Long): HiveWriter = {

    // we need a hive writer per thread (to different files of course), and a writer
    // per partition (as each partition is written to a different directory)
    val parts = PartitionPartsFn.rowPartitionParts(row, partitionKeyNames)
    val partPath = ops.partitionPathString(parts, tablePath)
    writers.getOrElseUpdate(partPath + writerId, {

      val filePath = new Path(partPath, "part_" + System.nanoTime() + "_" + writerId)
      logger.debug(s"Creating hive writer for $filePath")

      // if dynamic partition is enabled then we will try to createReader the hive partition automatically
      if (dynamicPartitioning) {
        if (parts.nonEmpty) {
          // we need to synchronize this, as its quite likely that when ioThreads>1 we have >1 thread
          // trying to createReader a partition at the same time. This is virtually guaranteed to happen if
          // the data is in any way sorted
          if (!createdPartitions.contains(partPath.toString())) {
            lock.synchronized {
              ops.createPartitionIfNotExists(dbName, tableName, parts)
              createdPartitions.add(partPath.toString())
            }
          }
        }
      } else if (!ops.partitionExists(dbName, tableName, parts)) {
        sys.error(s"Partition $partPath does not exist and dynamicPartitioning = false")
      }

      dialect.writer(fileSchema, filePath)
    })
  }
}