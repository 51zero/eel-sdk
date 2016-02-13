package io.eels.component.hive

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.mutable

case class HiveSink(dbName: String,
                    tableName: String,
                    props: HiveSinkProps = HiveSinkProps(),
                    partitionKeys: List[String] = Nil)
                   (implicit fs: FileSystem, hiveConf: HiveConf) extends Sink with StrictLogging {

  def withPartitions(first: String, rest: String*): HiveSink = copy(partitionKeys = (first +: rest).toList)

  override def writer: Writer = new Writer {
    logger.debug(s"Writing created; partitions=${partitionKeys.mkString(",")}")

    implicit val client = new HiveMetaStoreClient(hiveConf)

    var created = false

    // returns all the partitions for a given row, if a row does not have a value for a particular partition,
    // then that partition is skipped
    private def partitions(row: Row): Seq[Partition] = {
      partitionKeys.filter(row.contains).map(key => Partition(key, row(key)))
    }

    private def ensurePartitionsCreated(row: Row): Unit = {
      partitions(row).foreach(p => HiveOps.createPartition(dbName, tableName, p.name, p.value))
    }

    private def tablePath(row: Row): Path = new Path(HiveOps.location(dbName, tableName))

    private def partitionPath(row: Row): Path = {
      partitions(row).foldLeft(tablePath(row))((path, part) => new Path(path, part.unquotedDir))
    }

    private def ensureTableCreated(row: Row): Unit = {
      if (props.createTable && !created) {
        logger.debug(s"Ensuring table $tableName is created")
        val schema = FrameSchema(row.columns)
        HiveOps.createTable(dbName, tableName, schema, partitionKeys, props.format, overwrite = props.overwriteTable)
        created = true
      }
    }

    // we need an output stream per partition. Since the data can come in unordered, we need to
    // keep open a stream per partition path. This shouldn't be shared amongst threads until its made thread safe.
    var streams = mutable.Map.empty[Path, HiveWriter]

    private def getOrCreateHiveWriter(row: Row): HiveWriter = {
      val partPath = partitionPath(row)
      streams.getOrElseUpdate(partPath, {
        val filePath = new Path(partPath, "eel_" + System.nanoTime)
        logger.debug(s"Creating hive writer for $filePath")
        ensurePartitionsCreated(row)
        dialect.writer(FrameSchema(row.columns), filePath)
      })
    }

    lazy val dialect = {
      val format = HiveOps.tableFormat(dbName, tableName)
      logger.debug(s"Table format is $format")
      HiveDialect(format)
    }

    override def close(): Unit = {
      logger.debug("Closing hive sink writer")
      streams.values.foreach(_.close)
    }

    override def write(row: Row): Unit = {
      ensureTableCreated(row)
      val writer = getOrCreateHiveWriter(row)
      writer.write(row)
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