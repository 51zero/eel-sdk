package io.eels.component.hive

import com.sksamuel.scalax.io.Using
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.parquet.ParquetLogMute
import io.eels.{FrameSchema, HdfsIterator, Reader, Source}
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

import scala.collection.JavaConverters._

case class HiveSource(db: String, table: String,
                      props: HiveSourceProps = HiveSourceProps(),
                      partitionExprs: List[PartitionExpr] = Nil)
                     (implicit fs: FileSystem, hive: HiveConf)
  extends Source
    with StrictLogging
    with Using {
  ParquetLogMute()

  def withPartition(name: String, value: String): HiveSource = withPartition(name, "=", value)
  def withPartition(name: String, op: String, value: String): HiveSource = {
    val expr = op match {
      case "=" => PartitionEquals(name, value)
      case ">" => PartitionGt(name, value)
      case ">=" => PartitionGte(name, value)
      case "<" => PartitionLt(name, value)
      case "<=" => PartitionLte(name, value)
      case _ => sys.error(s"Unsupported op $op")
    }
    copy(partitionExprs = partitionExprs :+ expr)
  }

  private def createClient: HiveMetaStoreClient = new HiveMetaStoreClient(hive)

  private def isHidden(file: LocatedFileStatus): Boolean = {
    props.ignoreHiddenFiles && file.getPath.getName.matches(props.hiddenFilePattern)
  }

  // returns true if the file meets the partition exprs
  private def isEvaluated(file: LocatedFileStatus): Boolean = {
    // todo need better way of getting all partition info
    val partitions = Iterator.iterate(file.getPath.getParent)(path => path.getParent)
      .takeWhile(_ != null)
      .filter(_.getName.contains("="))
      .collect {
        case PartitionPart(name, value) => PartitionPart(name, value)
      }.toList
    partitionExprs.forall(_.eval(partitions))
  }

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
        ParquetLogMute()
        lazy val iterator = dialect.iterator(path, schema)
        override def close(): Unit = () // todo close dialect
      }
    }.toList
  }
}

case class HiveSourceProps(ignoreHiddenFiles: Boolean = true,
                           hiddenFilePattern: String = "_.*")

trait PartitionExpr {
  def eval(partitions: List[PartitionPart]): Boolean
}

case class PartitionEquals(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = partitions.contains(PartitionPart(name, value))
}

case class PartitionLt(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) < 0)
  }
}

case class PartitionLte(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) <= 0)
  }
}

case class PartitionGt(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) > 0)
  }
}

case class PartitionGte(name: String, value: String) extends PartitionExpr {
  override def eval(partitions: List[PartitionPart]): Boolean = {
    partitions.find(_.key == name).exists(_.value.compareTo(value) >= 0)
  }
}