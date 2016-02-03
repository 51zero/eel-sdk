package io.eels.component.hive

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, HdfsIterator, Reader, Row, SchemaType, Source}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.JavaConverters._

case class HiveSource(db: String, table: String, props: HiveSourceProps = HiveSourceProps())
                     (implicit fs: FileSystem, hive: HiveConf)
  extends Source
    with StrictLogging {

  override def reader: Reader = new Reader {

    val client = new HiveMetaStoreClient(hive)

    val t = client.getTable(db, table)
    logger.debug("table=" + t)

    val location = t.getSd.getLocation
    logger.info(s"Loading $db.$table files from $location")

    val format = t.getSd.getInputFormat
    logger.debug(s"Table format is $format")

    val dialect = HiveDialect(format)
    logger.debug(s"HiveDialect is $dialect")

    val keys = t.getPartitionKeys.asScala
    logger.debug("Partition keys=" + keys.mkString(", "))

    val schema = client.getSchema(db, table).asScala
    logger.debug("Loaded field schema " + schema.mkString(", "))

    val columns = schema.map { s =>
      Column(s.getName, SchemaType.String, false)
    }
    logger.info("Loaded columns " + columns.mkString(", "))

    logger.debug(s"Scanning $location, filtering=${props.ignoreHiddenFiles} pattern=${props.hiddenFilePattern}")
    val paths = HdfsIterator(fs.listFiles(new Path(location), true)).filter(_.isFile).filter { file =>
      props.noHiddenFiles || file.getPath.getName.matches(props.hiddenFilePattern)
    }.toList.map(_.getPath)
    logger.info(s"Found ${paths.size} files for table $db.$table")

    val iterators = paths.map(dialect.iterator(_)(fs))

    override val iterator: Iterator[Row] = {
      if (iterators.isEmpty) Iterator.empty
      else iterators.reduceLeft((a, b) => a ++ b).map(fields => Row(columns, fields))
    }

    override def close(): Unit = ()
  }
}

case class HiveSourceProps(ignoreHiddenFiles: Boolean = true, hiddenFilePattern: String = "_.*") {
  def noHiddenFiles: Boolean = !ignoreHiddenFiles
}