package io.eels.component.hive

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors, ArrayBlockingQueue}

import com.sksamuel.scalax.collection.BlockingQueueConcurrentIterator
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.mutable

case class HiveSink(dbName: String,
                    tableName: String,
                    props: HiveSinkProps = HiveSinkProps(),
                    ioThreads: Int = 4,
                    dynamicPartitioning: Boolean = true,
                    bufferSize: Int = 1000)
                   (implicit fs: FileSystem, hiveConf: HiveConf) extends Sink with StrictLogging {

  def withIOThreads(ioThreads: Int): HiveSink = copy(ioThreads = ioThreads)
  def withDynamicPartitioning(dynamicPartitioning: Boolean): HiveSink = copy(dynamicPartitioning = dynamicPartitioning)

  override def writer: Writer = {
    logger.debug(s"HiveSinkWriter created")

    implicit val client = new HiveMetaStoreClient(hiveConf)

    val dialect = {
      val format = HiveOps.tableFormat(dbName, tableName)
      logger.debug(s"Table format is $format")
      HiveDialect(format)
    }

    val tablePath = HiveOps.tablePath(dbName, tableName)

    val partitionKeyNames = HiveOps.partitionKeyNames(dbName, tableName)
    logger.debug("Dynamic partitioning enabled: " + partitionKeyNames.mkString(","))

    // returns all the partition parts for a given row, if a row doesn't contain a value
    // for a part then an error is thrown
    def parts(row: Row): Seq[PartitionPart] = {
      require(partitionKeyNames.forall(row.contains), "Row must contain all partitions " + partitionKeyNames)
      partitionKeyNames.map { name =>
        val value = row(name)
        require(!value.contains(" "), "Values for partition fields cannot contain spaces")
        PartitionPart(name, value)
      }
    }

    // we need an output stream per partition. Since the data can come in unordered, we need to
    // keep open a stream per partition path. This shouldn't be shared amongst threads until its made thread safe.
    val writers = mutable.Map.empty[Path, HiveWriter]

    def getOrCreateHiveWriter(row: Row): HiveWriter = {
      val partPath = HiveOps.partitionPath(dbName, tableName, parts(row), tablePath)
      writers.synchronized {
        writers.getOrElseUpdate(partPath, {
          val filePath = new Path(partPath, "eel_" + System.nanoTime)
          logger.debug(s"Creating hive writer for $filePath")
          if (dynamicPartitioning)
            HiveOps.createPartitionIfNotExists(dbName, tableName, parts(row))
          else if (!HiveOps.partitionExists(dbName, tableName, parts(row)))
            sys.error(s"Partition $partPath does not exist and dynamicPartitioning=false")
          dialect.writer(FrameSchema(row.columns), filePath)
        })
      }
    }

    import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable

    val queue = new ArrayBlockingQueue[Row](bufferSize)
    val latch = new CountDownLatch(ioThreads)
    val executor = Executors.newFixedThreadPool(ioThreads)
    val count = new AtomicLong(0)

    for ( k <- 1 to ioThreads ) {
      executor.submit {
        logger.info(s"HiveSink thread $k started")
        try {
          BlockingQueueConcurrentIterator(queue, Row.Sentinel).foreach { row =>
            val writer = getOrCreateHiveWriter(row)
            writer.synchronized {
              writer.write(row)
            }
            val k = count.incrementAndGet()
            if (k % 10000 == 0)
              logger.debug(s"Written $k / ? =>")
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
        queue.put(Row.Sentinel)
        executor.awaitTermination(1, TimeUnit.HOURS)
      }

      override def write(row: Row): Unit = {
        queue.put(row)
      }
    }
  }
}

case class PartitionPart(key: String, value: String) {
  def unquotedDir = s"$key=$value"
}

object PartitionPart {
  def unapply(path: Path): Option[(String, String)] = unapply(path.getName)
  def unapply(str: String): Option[(String, String)] = str.split('=') match {
    case Array(a, b) => Some((a, b))
    case _ => None
  }
}

case class HiveSinkProps(createTable: Boolean = false,
                         overwriteTable: Boolean = false,
                         format: HiveFormat = HiveFormat.Text)