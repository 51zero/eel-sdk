package io.eels.component.hive


import io.eels.Row
import io.eels.SinkWriter
import io.eels.schema.Schema
import io.eels.util.Logging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class HiveSinkWriter(sourceSchema: Schema,
                     metastoreSchema: Schema,
                     val dbName: String,
                     val tableName: String,
                     ioThreads: Int,
                     val dialect: HiveDialect,
                     val dynamicPartitioning: Boolean,
                     includePartitionsInData: Boolean,
                     bufferSize: Int,
                     val fs: FileSystem,
                     client: IMetaStoreClient) : SinkWriter, Logging {

  val ops = HiveOps(client)
  val tablePath = ops.tablePath(dbName, tableName)
  val lock = Any()

  // these will be in lower case
  val partitionKeyNames = ops.partitionKeyNames(dbName, tableName)

  // the file schema is the metastore schema with the partition columns removed. This is because the
  // partition columns are not written to the file (they are taken from the partition itself)
  // this can be overriden with the includePartitionsInData option
  val fileSchema = if (includePartitionsInData || partitionKeyNames.isEmpty()) metastoreSchema
  else partitionKeyNames.fold(metastoreSchema, { schema, name ->
    schema.removeField(name, caseSensitive = false)
  })

  // these are the indexes of the cells to skip in each row because they are partition values
  // we build up the ones to skip, then we can make an array that contains only the ones to keep
  // this array is used to iterate over using the indexes to pick out the values from the row quickly
  val indexesToSkip = if (includePartitionsInData) emptyList() else partitionKeyNames.map { sourceSchema.indexOf(it) }
  val indexesToWrite = Array(sourceSchema.size()) { it }.filterNot { indexesToSkip.contains(it) }

  // Since the data can come in unordered, we need to keep open a stream per partition path otherwise we'd
  // be opening and closing streams frequently.
  // We also can't share a writer amongst threads for obvious reasons otherwise we'd just have a single
  // writer being used for all data. So the solution is a hive writer per thread per partition, each
  // writing to their own files.
  val writers = mutableMapOf<String, HiveWriter>()

  // this contains all the partitions we've checked.
  // No need for multiple threads to keep hitting the meta store to check on the same partition paths
  val createdPartitions = ConcurrentSkipListSet<String>()

  // we buffer incoming data into this queue, so that slow writing doesn't unncessarily
  // block threads feeding this sink
  val buffer = LinkedBlockingQueue<Row>(bufferSize)
  val latch = CountDownLatch(1)

  val writerPool = Executors.newFixedThreadPool(ioThreads)!!

  init {
    val base = System.nanoTime()
    logger.debug("HiveSinkWriter created; timestamp=$base; dynamicPartitioning=$dynamicPartitioning; ioThreads=$ioThreads; includePartitionsInData=$includePartitionsInData")

    writerPool.submit {
      // this won't work for multiple threads, as the first thread will read the pill
      // todo make this multithreaded
      generateSequence { buffer.take() }.takeWhile { it != Row.PoisonPill }.forEach { row ->
        // todo make this multithreaded by using some unique id
        val writer = getOrCreateHiveWriter(row, 1L)
        // need to strip out any partition information from the written data
        // keeping this as a list as I want it ordered and no need to waste cycles on an ordered map
        val rowToWrite = if (indexesToSkip.isEmpty()) row else {
          val values = mutableListOf<Any?>()
          for (k in indexesToWrite) {
            values.add(row.get(k))
          }
          Row(fileSchema, values)
        }
        writer.write(rowToWrite)
      }
    }
    writerPool.shutdown()
  }

  override fun write(row: Row): Unit = buffer.put(row)

  override fun close(): Unit {
    logger.debug("Request to close hive sink writer")
    buffer.put(Row.PoisonPill)
    writerPool.awaitTermination(1, TimeUnit.DAYS)
    logger.debug("All writers have finished writing rows; closing writers to ensure flush")
    writers.values.forEach { it.close() }
  }

  fun getOrCreateHiveWriter(row: Row, writerId: Long): HiveWriter {

    // we need a hive writer per thread (to different files of course), and a writer
    // per partition (as each partition is written to a different directory)
    val parts = PartitionPartsFn.rowPartitionParts(row, partitionKeyNames)
    val partPath = ops.partitionPathString(parts, tablePath)
    return writers.getOrPut(partPath + writerId, {

      val filePath = Path(partPath, "part_" + System.nanoTime() + "_" + writerId)
      logger.debug("Creating hive writer for $filePath")

      // if dynamic partition is enabled then we will try to create the hive partition automatically
      if (dynamicPartitioning) {
        if (parts.isNotEmpty()) {
          // we need to synchronize this, as its quite likely that when ioThreads>1 we have >1 thread
          // trying to create a partition at the same time. This is virtually guaranteed to happen if
          // the data is in any way sorted
          if (!createdPartitions.contains(partPath.toString())) {
            synchronized(lock) {
              ops.createPartitionIfNotExists(dbName, tableName, parts)
              createdPartitions.add(partPath.toString())
            }
          }
        }
      } else if (!ops.partitionExists(dbName, tableName, parts)) {
        error("Partition $partPath does not exist and dynamicPartitioning = false")
      }

      dialect.writer(fileSchema, filePath, fs)
    })
  }
}