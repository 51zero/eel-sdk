package io.eels.component.hive

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, HdfsIterator, Part, Row, Source}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.collection.JavaConverters._

case class HiveSource(db: String, table: String, props: HiveSourceProps = HiveSourceProps())
                     (implicit fs: FileSystem, hive: HiveConf)
  extends Source
    with StrictLogging
    with Using {

  def createClient: HiveMetaStoreClient = new HiveMetaStoreClient(hive)

  def isHidden(file: LocatedFileStatus): Boolean = {
    props.ignoreHiddenFiles && file.getPath.getName.matches(props.hiddenFilePattern)
  }

  def visiblePaths(location: String): Seq[Path] = {
    logger.debug(s"Scanning $location, filtering=${props.ignoreHiddenFiles} pattern=${props.hiddenFilePattern}")
    val files = HdfsIterator(fs.listFiles(new Path(location), true)).filter(_.isFile).toList
    logger.debug(s"Found ${files.size} files before filtering")

    val paths = files.filterNot(isHidden).map(_.getPath)
    logger.info(s"Found ${paths.size} files after filtering")
    paths
  }

  override def schema: FrameSchema = {
    using(createClient) { client =>

      val s = client.getSchema(db, table).asScala
      logger.debug("Loaded hive schema " + s.mkString(", "))

      val frameSchema = FrameSchemaBuilder(s)
      logger.debug("Generated frame schema=" + frameSchema)

      frameSchema
    }
  }

  override def parts: Seq[Part] = {

    val client = createClient
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

    val s = client.getSchema(db, table).asScala
    logger.debug("Loaded hive schema " + s.mkString(", "))

    val frameSchema = FrameSchemaBuilder(s)
    logger.debug("Generated frame schema=" + frameSchema)

    val paths = visiblePaths(location)

    client.close()

    paths.map {
      path =>
        new Part {
          override def iterator: Iterator[Row] = dialect.iterator(path, frameSchema)
        }
    }
  }

}

case class HiveSourceProps(ignoreHiddenFiles: Boolean = true, hiddenFilePattern: String = "_.*")