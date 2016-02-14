package io.eels.component.hive

import java.io.{InputStreamReader, BufferedReader, InputStream}

import com.github.tototoshi.csv.{CSVWriter, DefaultCSVFormat}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Field, Row, FrameSchema}
import org.apache.hadoop.fs.{Path, FileSystem}

object TextHiveDialect extends HiveDialect with StrictLogging {

  val delimiter = '\u0001'

  override def iterator(path: Path, schema: FrameSchema)
                       (implicit fs: FileSystem): Iterator[Row] = new Iterator[Row] {
    lazy val in = fs.open(path)
    lazy val iter = lineIterator(in)
    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = {
      val fields = iter.next.split(delimiter).map(Field.apply).toList.padTo(schema.columns.size, null)
      Row(schema.columns, fields)
    }
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
      csv.writeRow(row.fields.map(_.value))
    }

    override def close(): Unit = {
      csv.close()
      out.close()
    }
  }
}
