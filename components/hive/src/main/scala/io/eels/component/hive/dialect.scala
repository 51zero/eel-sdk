package io.eels.component.hive

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.parquet.ParquetIterator
import io.eels.{Field, FrameSchema, Row}
import org.apache.hadoop.fs.{FileSystem, Path}

trait HiveDialect extends StrictLogging {
  def iterator(path: Path, schema: FrameSchema)(implicit fs: FileSystem): Iterator[Row]
}

object HiveDialect {
  def apply(format: String): HiveDialect = format match {
    case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" => ParquetHiveDialect
    case "org.apache.hadoop.mapred.TextInputFormat" | "org.apache.hadoop.mapreduce.lib.input" => TextHiveDialect
    case other => sys.error("Unknown hive input format: " + other)
  }
}

object TextHiveDialect extends HiveDialect {

  val delimiter = '\001'

  override def iterator(path: Path, schema: FrameSchema)
                       (implicit fs: FileSystem): Iterator[Row] = new Iterator[Row] {
    lazy val in = fs.open(path)
    lazy val iter = lineIterator(in)
    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = {
      val fields = iter.next.split(delimiter).map(Field.apply).toList.padTo(schema.columns.size, null)
      logger.debug("Fields=" + fields)
      Row(schema.columns, fields)
    }
  }

  def lineIterator(in: InputStream): Iterator[String] = {
    val buff = new BufferedReader(new InputStreamReader(in))
    Iterator.continually(buff.readLine).takeWhile(_ != null)
  }
}

object ParquetHiveDialect extends HiveDialect {
  override def iterator(path: Path, schema: FrameSchema)
                       (implicit fs: FileSystem): Iterator[Row] = new Iterator[Row] {
    lazy val iter = ParquetIterator(path)
    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = {
      val map = iter.next.toMap
      val fields = for ( column <- schema.columns ) yield Field(map.getOrElse(column.name, null))
      Row(schema.columns, fields)
    }
  }
}