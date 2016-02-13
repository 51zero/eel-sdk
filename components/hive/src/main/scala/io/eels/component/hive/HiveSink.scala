package io.eels.component.hive

import java.util.UUID

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, Row, Sink, Writer}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
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
      partitions(row).foreach(p => HiveOps.createPartition(dbName, tableName, p.key, p.value))
    }

    private def tablePath(row: Row): Path = new Path(HiveOps.location(dbName, tableName))

    private def partitionPath(row: Row): Path = {
      partitions(row).foldLeft(tablePath(row))((path, part) => new Path(path, part.dirName))
    }

    private def ensureTableCreated(row: Row): Unit = {
      logger.debug(s"Ensuring table $tableName is created")
      if (props.createTable && !created) {
        val schema = FrameSchema(row.columns)
        HiveOps.createTable(dbName, tableName, schema, partitionKeys, props.format, props.overwriteTable)
        created = true
      }
    }

    // we need an output stream per partition. Since the data can come in unordered, we need to
    // keep open a stream per partition path. This shouldn't be shared amongst threads until its made thread safe.
    var streams = mutable.Map.empty[Path, HiveWriter]

    private def getOrCreateHiveWriter(row: Row): HiveWriter = {
      val partPath = partitionPath(row)
      streams.getOrElseUpdate(partPath, {
        val filePath = new Path(partPath, UUID.randomUUID.toString)
        logger.debug(s"Creating stream for $filePath")
        ensurePartitionsCreated(row)
        dialect.writer(FrameSchema(row.columns), filePath)
      })
    }

    lazy val dialect = {
      val format = HiveOps.tableFormat(dbName, tableName)
      logger.debug(s"Table format is $format")
      HiveDialect(format)
    }

    override def close(): Unit = streams.values.foreach(_.close)

    override def write(row: Row): Unit = {
      ensureTableCreated(row)
      val writer = getOrCreateHiveWriter(row)
      writer.write(row)
    }
  }
}

case class Partition(key: String, value: String) {
  def dirName = s"$key=$value"
}

case class HiveSinkProps(createTable: Boolean = false,
                         overwriteTable: Boolean = false,
                         format: HiveFormat = HiveFormat.Text)