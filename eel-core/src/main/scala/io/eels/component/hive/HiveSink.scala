package io.eels.component.hive

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, Executors, TimeUnit}

import com.sksamuel.scalax.collection.BlockingQueueConcurrentIterator
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, InternalRow, Sink, Writer}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.JavaConverters._
import scala.collection.mutable

case class HiveSink(dbName: String,
                    tableName: String,
                    ioThreads: Int = 4,
                    dynamicPartitioning: Boolean = true,
                    createTable: Boolean = false,
                    overwriteTable: Boolean = false,
                    format: HiveFormat = HiveFormat.Text,
                    bufferSize: Int = 1000)
                   (implicit fs: FileSystem, hiveConf: HiveConf) extends Sink with StrictLogging {
  logger.info(s"Created HiveSink; createTable=$createTable, overwriteTable=$overwriteTable; format=$format")

  val config = ConfigFactory.load()
  val includePartitionsInData = config.getBoolean("hive.includePartitionsInData")

  def withIOThreads(ioThreads: Int): HiveSink = copy(ioThreads = ioThreads)
  def withDynamicPartitioning(dynamicPartitioning: Boolean): HiveSink = copy(dynamicPartitioning = dynamicPartitioning)

  /**
    * Returns the schema for the hive destination as an Eel format schema object.
    */
  def schema(implicit client: HiveMetaStoreClient): FrameSchema = {
    val schema = client.getSchema(dbName, tableName)
    FrameSchemaFn(schema.asScala)
  }

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

    val targetSchema = HiveSink.this.schema

    // we need an output stream per partition. Since the data can come in unordered, we need to
    // keep open a stream per partition path. This shouldn't be shared amongst threads until its made thread safe.
    val writers = mutable.Map.empty[Path, HiveWriter]

    def getOrCreateHiveWriter(row: InternalRow, sourceSchema: FrameSchema): HiveWriter = {
      val parts = RowPartitionParts(row, partitionKeyNames, sourceSchema)
      val partPath = HiveOps.partitionPath(dbName, tableName, parts, tablePath)
      writers.synchronized {
        writers.getOrElseUpdate(partPath, {
          val filePath = new Path(partPath, "eel_" + System.nanoTime)
          logger.debug(s"Creating hive writer for $filePath")
          if (dynamicPartitioning) {
            if (parts.nonEmpty)
              HiveOps.createPartitionIfNotExists(dbName, tableName, parts)
          } else if (!HiveOps.partitionExists(dbName, tableName, parts)) {
            sys.error(s"Partition $partPath does not exist and dynamicPartitioning=false")
          }
          dialect.writer(sourceSchema, targetSchema, filePath)
        })
      }
    }

    import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable

    val queue = new ArrayBlockingQueue[InternalRow](bufferSize)
    val latch = new CountDownLatch(ioThreads)
    val executor = Executors.newFixedThreadPool(ioThreads)
    val count = new AtomicLong(0)
    var schema: FrameSchema = null


    for ( k <- 1 to ioThreads ) {
      executor.submit {
        logger.info(s"HiveSink thread $k started")
        try {
          BlockingQueueConcurrentIterator(queue, InternalRow.PoisonPill).foreach { row =>
            val writer = getOrCreateHiveWriter(row, schema)
            writer.synchronized {
              // need to strip out any partition information from the written data
              // keeping this as a list as I want it ordered and no need to waste cycles on an ordered map
              if (includePartitionsInData) {
                writer.write(row)
              } else {
                val zipped = schema.columnNames.zip(row)
                val rowWithoutPartitions = zipped.filterNot(partitionKeyNames contains _._1)
                writer.write(rowWithoutPartitions.unzip._2)
              }
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
        queue.put(InternalRow.PoisonPill)
        executor.awaitTermination(1, TimeUnit.HOURS)
      }

      override def write(row: InternalRow, _schema: FrameSchema): Unit = {
        schema = _schema
        queue.put(row)
      }
    }
  }
}

object HiveSink {

  def apply(dbName: String, tableName: String, params: Map[String, List[String]])
           (implicit fs: FileSystem, hiveConf: HiveConf): HiveSink = {

    val createTable = params.get("createTable").map(_.head).getOrElse("false") == "true"
    val overwriteTable = params.get("overwriteTable").map(_.head).getOrElse("false") == "true"
    val dynamicPartitioning = params.get("dynamicPartitioning").map(_.head).getOrElse("false") == "true"

    val format = params.get("format").map(_.head).getOrElse("text").toLowerCase match {
      case "avro" => HiveFormat.Avro
      case "orc" => HiveFormat.Orc
      case "parquet" => HiveFormat.Parquet
      case _ => HiveFormat.Text
    }

    HiveSink(
      dbName,
      tableName,
      createTable = createTable,
      overwriteTable = overwriteTable,
      dynamicPartitioning = dynamicPartitioning,
      format = format
    )
  }
}

@deprecated("will be replaced with PartitionKeyValue", "0.24.0")
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

// returns all the partition parts for a given row, if a row doesn't contain a value
// for a part then an error is thrown
object RowPartitionParts {
  def apply(row: InternalRow, partNames: Seq[String], schema: FrameSchema): List[PartitionPart] = {
    require(partNames.forall(schema.columnNames.contains), "FrameSchema must contain all partitions " + partNames)
    partNames.map { name =>
      val index = schema.indexOf(name)
      val value = row(index)
      require(!value.toString.contains(" "), "Values for partition fields cannot contain spaces")
      PartitionPart(name, value.toString)
    }.toList
  }
}