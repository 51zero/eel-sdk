package io.eels.component.hive

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, Executors, TimeUnit}

import com.sksamuel.scalax.collection.BlockingQueueConcurrentIterator
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Schema, InternalRow, Sink, Writer}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.JavaConverters._
import scala.collection.mutable

case class HiveSink(private val dbName: String,
                    private val tableName: String,
                    private val ioThreads: Int = 4,
                    private val dynamicPartitioning: Boolean = true,
                    private val bufferSize: Int = 1000)
                   (implicit fs: FileSystem, hiveConf: HiveConf) extends Sink with StrictLogging {

  val config = ConfigFactory.load()
  val includePartitionsInData = config.getBoolean("eel.hive.includePartitionsInData")

  def withIOThreads(ioThreads: Int): HiveSink = copy(ioThreads = ioThreads)
  def withDynamicPartitioning(dynamicPartitioning: Boolean): HiveSink = copy(dynamicPartitioning = dynamicPartitioning)
  def withBufferSize(bufferSize: Int): HiveSink = copy(bufferSize = bufferSize)

  /**
    * Returns the schema for the hive destination as an Eel format schema object.
    */
  def schema(implicit client: HiveMetaStoreClient): Schema = {
    val schema = client.getSchema(dbName, tableName)
    HiveSchemaFns.fromHiveFields(schema.asScala)
  }

  override def writer: Writer = {
    val base = System.nanoTime
    logger.debug(s"HiveSinkWriter created; timestamp=$base")

    implicit val client = new HiveMetaStoreClient(hiveConf)

    val dialect = {
      val format = HiveOps.tableFormat(dbName, tableName)
      logger.debug(s"Table format is $format")
      HiveDialect(format)
    }

    val tablePath = HiveOps.tablePath(dbName, tableName)
    val lock = new Object {}

    val partitionKeyNames = HiveOps.partitionKeyNames(dbName, tableName)
    logger.debug("Dynamic partitioning enabled: " + partitionKeyNames.mkString(","))

    // Since the data can come in unordered, we need to keep open a stream per partition path.
    // This shouldn't be shared amongst threads so that we can increase throughput by increasing the number
    // of threads (if it was shared, then if a single path we might only have one writer for all the output).
    // the key should include the thread count so that each thread has its own unique writer
    val writers = mutable.Map.empty[String, HiveWriter]

    def getOrCreateHiveWriter(row: InternalRow, sourceSchema: Schema, k: Long): HiveWriter = {

      val parts = RowPartitionParts(row, partitionKeyNames, sourceSchema)
      val partPath = HiveOps.partitionPath(dbName, tableName, parts, tablePath)
      writers.getOrElseUpdate(partPath.toString + "_" + k, {

        // this is not thread safe, so each thread needs its own copy when in here
        implicit val client = new HiveMetaStoreClient(hiveConf)

        val filePath = new Path(partPath, "part_" + System.nanoTime + "_" + k)
        logger.debug(s"Creating hive writer for $filePath")
        if (dynamicPartitioning) {
          if (parts.nonEmpty) {
            // we need to synchronize this, as its quite likely that when ioThreads>1 we have >1 thread
            // trying to create a partition at the same time. This is virtually guaranteed to happen if the data
            // is in any way sorted
            lock.synchronized {
              HiveOps.createPartitionIfNotExists(dbName, tableName, parts)
            }
          }
        } else if (!HiveOps.partitionExists(dbName, tableName, parts)) {
          sys.error(s"Partition $partPath does not exist and dynamicPartitioning = false")
        }
        val targetSchema = if (includePartitionsInData) {
          HiveSink.this.schema
        } else {
          partitionKeyNames.foldLeft(HiveSink.this.schema)((schema, name) => schema.removeColumn(name))
        }
        dialect.writer(targetSchema, filePath)
      })
    }

    import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable

    val queue = new ArrayBlockingQueue[InternalRow](bufferSize)
    val latch = new CountDownLatch(ioThreads)
    val executor = Executors.newFixedThreadPool(ioThreads)
    val count = new AtomicLong(0)
    var schema: Schema = null

    for (k <- 0 until ioThreads) {
      executor.submit {
        logger.info(s"HiveSink thread $k started")
        try {
          BlockingQueueConcurrentIterator(queue, InternalRow.PoisonPill).foreach { row =>
            val writer = getOrCreateHiveWriter(row, schema, k)
            // need to strip out any partition information from the written data
            // keeping this as a list as I want it ordered and no need to waste cycles on an ordered map
            if (includePartitionsInData) {
              writer.write(row)
            } else {
              val zipped = schema.columnNames.zip(row)
              val rowWithoutPartitions = zipped.filterNot(partitionKeyNames contains _._1)
              writer.write(rowWithoutPartitions.unzip._2)
            }
            val j = count.incrementAndGet()
            if (j % 10000 == 0)
              logger.debug(s"Written $j / ? =>")
          }
        } catch {
          case e: Throwable => logger.error("Error writing row", e)
        } finally {
          logger.info(s"Sink thread $k completed")
          latch.countDown()
        }
      }
    }

    executor.submit {
      latch.await(1, TimeUnit.DAYS)
      logger.debug(s"Latch released; closing ${writers.size} hive writers")
      writers.values.foreach(_.close)
    }

    executor.shutdown()

    new Writer {
      override def close(): Unit = {
        logger.debug("Request to close hive sink writer")
        queue.put(InternalRow.PoisonPill)
        executor.awaitTermination(1, TimeUnit.HOURS)
      }

      override def write(row: InternalRow, _schema: Schema): Unit = {
        schema = _schema
        queue.put(row)
      }
    }
  }
}

object HiveSink {

  def apply(dbName: String, tableName: String, params: Map[String, List[String]])
           (implicit fs: FileSystem, hiveConf: HiveConf): HiveSink = {
    val dynamicPartitioning = params.get("dynamicPartitioning").map(_.head).getOrElse("false") == "true"
    HiveSink(
      dbName,
      tableName,
      dynamicPartitioning = dynamicPartitioning
    )
  }
}

// returns all the partition parts for a given row, if a row doesn't contain a value
// for a part then an error is thrown
object RowPartitionParts {
  def apply(row: InternalRow, partNames: Seq[String], schema: Schema): List[PartitionPart] = {
    require(partNames.forall(schema.columnNames.contains), "FrameSchema must contain all partitions " + partNames)
    partNames.map { name =>
      val index = schema.indexOf(name)
      val value = row(index)
      require(!value.toString.contains(" "), "Values for partition fields cannot contain spaces")
      PartitionPart(name, value.toString)
    }.toList
  }
}