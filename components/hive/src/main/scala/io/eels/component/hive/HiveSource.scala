package io.eels.component.hive

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, Field, HdfsIterator, Reader, Row, SchemaType, Source}
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

    // todo, open one at a time
    val streams = paths.map(fs.open)

    def inputIterator(in: InputStream): Iterator[String] = {
      val buff = new BufferedReader(new InputStreamReader(in))
      Iterator.continually(buff.readLine).takeWhile(_ != null)
    }

    override val iterator: Iterator[Row] = {
      if (streams.isEmpty) Iterator.empty
      else {
        streams.map(inputIterator).reduceLeft((a, b) => a ++ b).map { line =>
          val fields = line.split('\001').map(Field.apply).toList
          logger.debug("Fields=" + fields)
          Row(columns, fields)
        }
      }
    }

    override def close(): Unit = streams.foreach(_.close)
  }
}

case class HiveSourceProps(ignoreHiddenFiles: Boolean = true, hiddenFilePattern: String = "_.*") {
  def noHiddenFiles: Boolean = !ignoreHiddenFiles
}