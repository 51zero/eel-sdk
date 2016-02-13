package io.eels.component.hive

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, HdfsIterator, Reader, Source}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

import scala.collection.JavaConverters._

case class HiveSource(db: String, table: String,
                      props: HiveSourceProps = HiveSourceProps(),
                      partitions: List[PartitionExpr] = Nil)
                     (implicit fs: FileSystem, hive: HiveConf)
  extends Source
    with StrictLogging
    with Using {

  def withPartition(name: String, value: String): HiveSource = {
    copy(partitions = partitions :+ PartitionEquals(name, value))
  }

  private def createClient: HiveMetaStoreClient = new HiveMetaStoreClient(hive)

  private def isHidden(file: LocatedFileStatus): Boolean = {
    props.ignoreHiddenFiles && file.getPath.getName.matches(props.hiddenFilePattern)
  }

  // returns true if the file meets the exprs
  private def isEvaluated(file: LocatedFileStatus): Boolean = partitions.forall(_.eval(file.getPath.toString))

  private def visiblePaths(location: String): Seq[Path] = {
    logger.debug(s"Scanning $location, filtering=${props.ignoreHiddenFiles} pattern=${props.hiddenFilePattern}")
    val files = HdfsIterator(fs.listFiles(new Path(location), true)).filter(_.isFile).toList
    logger.debug(s"Found ${files.size} files before filtering")

    val paths = files.filterNot(isHidden).filter(isEvaluated).map(_.getPath)
    logger.info(s"Found ${paths.size} files after filtering")
    paths
  }

  override def schema: FrameSchema = {
    using(createClient) { client =>

      val s = client.getSchema(db, table).asScala
      logger.debug("Loaded hive schema " + s.mkString(", "))

      val frameSchema = FrameSchemaFn(s)
      logger.debug("Generated frame schema=" + frameSchema)

      frameSchema
    }
  }

  private def dialect(t: Table): HiveDialect = {

    val format = t.getSd.getInputFormat
    logger.debug(s"Table format is $format")

    val dialect = HiveDialect(format)
    logger.debug(s"HiveDialect is $dialect")

    dialect
  }

  private def pathsToBeLoaded(t: Table): Seq[Path] = {
    val location = t.getSd.getLocation
    logger.info(s"Scanning $location for files")

    val keys = t.getPartitionKeys.asScala
    logger.debug("Partition keys=" + keys.mkString(", "))

    val paths = visiblePaths(location)
    paths
  }

  override def readers: Seq[Reader] = {

    val (schema, dialect, paths) = using(createClient) { client =>
      val t = client.getTable(db, table)
      val schema = this.schema
      val dialect = this.dialect(t)
      val paths = this.pathsToBeLoaded(t)
      (schema, dialect, paths)
    }

    paths.map { path =>
      new Reader {
        lazy val iterator = dialect.iterator(path, schema)
        override def close(): Unit = () // todo close dialect
      }
    }.toList
  }
}

case class HiveSourceProps(ignoreHiddenFiles: Boolean = true,
                           hiddenFilePattern: String = "_.*",
                           concurrentReads: Int = 8)

trait PartitionExpr {
  def eval(file: String): Boolean
}

case class PartitionEquals(name: String, value: String) extends PartitionExpr {
  override def eval(file: String): Boolean = file.toLowerCase.contains(s"$name=$value".toLowerCase)
}