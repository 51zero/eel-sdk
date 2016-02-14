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
                    partitionKeys: List[String] = Nil,
                    ioThreads: Int = 1,
                    bufferSize: Int = 1000)
                   (implicit fs: FileSystem, hiveConf: HiveConf) extends Sink with StrictLogging {

  def withPartitions(first: String, rest: String*): HiveSink = copy(partitionKeys = (first +: rest).toList)

  def withIOThreads(ioThreads: Int): HiveSink = copy(ioThreads = ioThreads)

  override def writer: Writer = {
    logger.debug(s"HiveSinkWriter created; partitions=${partitionKeys.mkString(",")}")

    implicit val client = new HiveMetaStoreClient(hiveConf)

    val dialect = {
      val format = HiveOps.tableFormat(dbName, tableName)
      logger.debug(s"Table format is $format")
      HiveDialect(format)
    }

    val location = HiveOps.location(dbName, tableName)
    logger.debug(s"Table location $location")

    var tableCreated = false

    // returns all the partitions for a given row, if a row does not have a value for a particular partition,
    // then that partition is skipped
    def partitions(row: Row): Seq[Partition] = {
      partitionKeys.filter(row.contains).map(key => Partition(key, row(key)))
    }

    def ensurePartitionsCreated(row: Row): Unit = {
      partitions(row).foreach(p => HiveOps.createPartition(dbName, tableName, p.name, p.value))
    }

    def tablePath(row: Row): Path = new Path(location)

    def partitionPath(row: Row): Path = {
      partitions(row).foldLeft(tablePath(row))((path, part) => new Path(path, part.unquotedDir))
    }

    def ensureTableCreated(row: Row): Unit = {
      if (props.createTable && !tableCreated) {
        logger.debug(s"Ensuring table $tableName is created")
        val schema = FrameSchema(row.columns)
        HiveOps.createTable(dbName, tableName, schema, partitionKeys, props.format, overwrite = props.overwriteTable)
        tableCreated = true
      }
    }

    // we need an output stream per partition. Since the data can come in unordered, we need to
    // keep open a stream per partition path. This shouldn't be shared amongst threads until its made thread safe.
    val writers = mutable.Map.empty[Path, HiveWriter]

    def getOrCreateHiveWriter(row: Row): HiveWriter = {
      val partPath = partitionPath(row)
      writers.getOrElseUpdate(partPath, {
        val filePath = new Path(partPath, "eel_" + System.nanoTime)
        logger.debug(s"Creating hive writer for $filePath")
        ensurePartitionsCreated(row)
        dialect.writer(FrameSchema(row.columns), filePath)
      })
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
            writer.write(row)
            val k = count.incrementAndGet()
            if (k % 10000 == 0)
              logger.debug(s"Writer Buffer=>HiveDialect $k/? =>")
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
        ensureTableCreated(row)
        queue.put(row)
      }
    }
  }
}

case class Partition(name: String, value: String) {
  def unquotedDir = s"$name=$value"
}

object Partition {
  def unapply(path: Path): Option[(String, String)] = unapply(path.getName)
  def unapply(str: String): Option[(String, String)] = str.split('=') match {
    case Array(a, b) => Some((a, b))
    case _ => None
  }
}

case class HiveSinkProps(createTable: Boolean = false,
                         overwriteTable: Boolean = false,
                         format: HiveFormat = HiveFormat.Text)