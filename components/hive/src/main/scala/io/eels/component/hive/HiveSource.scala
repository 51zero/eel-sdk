package io.eels.component.hive

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{HdfsIterator, Reader, Row, Source}
import org.apache.hadoop.fs.{LocatedFileStatus, FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.JavaConverters._

case class HiveSource(db: String, table: String, props: HiveSourceProps = HiveSourceProps())
                     (implicit fs: FileSystem, hive: HiveConf)
  extends Source
    with StrictLogging {

  def isHidden(file: LocatedFileStatus): Boolean = {
    props.ignoreHiddenFiles && file.getPath.getName.matches(props.hiddenFilePattern)
  }

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
    logger.debug("Loaded hive schema " + schema.mkString(", "))

    val frameSchema = FrameSchemaBuilder(schema)
    logger.debug("Generated frame schema=" + frameSchema)

    logger.debug(s"Scanning $location, filtering=${props.ignoreHiddenFiles} pattern=${props.hiddenFilePattern}")
    val files = HdfsIterator(fs.listFiles(new Path(location), true)).filter(_.isFile).toList
    logger.debug(s"Found ${files.size} files before filtering")

    val paths = files.filterNot(isHidden).map(_.getPath)
    logger.info(s"Found ${paths.size} files after filtering")

    val iterators = paths.map(dialect.iterator(_, frameSchema)(fs))

    override val iterator: Iterator[Row] = {
      if (iterators.isEmpty) Iterator.empty
      else iterators.reduceLeft((a, b) => a ++ b)
    }

    override def close(): Unit = ()
  }
}

case class HiveSourceProps(ignoreHiddenFiles: Boolean = true, hiddenFilePattern: String = "_.*")