package io.eels.component.hive.dialect

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.github.tototoshi.csv.{CSVWriter, DefaultCSVFormat}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.{FrameSchema, Row}
import org.apache.hadoop.fs.{FileSystem, Path}

object TextHiveDialect extends HiveDialect with StrictLogging {

  val delimiter = '\u0001'

  override def iterator(path: Path, schema: FrameSchema)
                       (implicit fs: FileSystem): Iterator[Row] = new Iterator[Row] {
    lazy val in = fs.open(path)
    lazy val iter = lineIterator(in)
    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = iter.next.split(delimiter).padTo(schema.columns.size, null).toSeq
  }

  def lineIterator(in: InputStream): Iterator[String] = {
    val buff = new BufferedReader(new InputStreamReader(in))
    Iterator.continually(buff.readLine).takeWhile(_ != null)
  }

  override def writer(schema: FrameSchema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = new HiveWriter {
    logger.debug(s"Creating text writer for $path with delimiter=${TextHiveDialect.delimiter}")

    val out = fs.create(path, false)
    val csv = CSVWriter.open(out)(new DefaultCSVFormat {
      override val delimiter: Char = TextHiveDialect.delimiter
      override val lineTerminator: String = "\n"
    })

    override def write(row: Row): Unit = {
      csv.writeRow(row)
    }

    override def close(): Unit = {
      csv.close()
      out.close()
    }
  }
}
