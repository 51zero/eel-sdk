package io.eels.component.hive.dialect

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.github.tototoshi.csv.{CSVWriter, DefaultCSVFormat}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.hive.{HiveDialect, HiveWriter}
import io.eels.{FrameSchema, InternalRow}
import org.apache.hadoop.fs.{FileSystem, Path}

object TextHiveDialect extends HiveDialect with StrictLogging {

  val delimiter = '\u0001'

  override def iterator(path: Path, schema: FrameSchema, ignored: Seq[String])
                       (implicit fs: FileSystem): Iterator[InternalRow] = new Iterator[InternalRow] {
    lazy val in = fs.open(path)
    lazy val iter = lineIterator(in)
    override def hasNext: Boolean = iter.hasNext
    override def next(): InternalRow = iter.next.split(delimiter).padTo(schema.columns.size, null).toSeq
  }

  def lineIterator(in: InputStream): Iterator[String] = {
    val buff = new BufferedReader(new InputStreamReader(in))
    Iterator.continually(buff.readLine).takeWhile(_ != null)
  }

  override def writer(sourceSchema: FrameSchema, targetSchema: FrameSchema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = new HiveWriter {
    logger.debug(s"Creating text writer for $path with delimiter=${TextHiveDialect.delimiter}")

    val out = fs.create(path, false)
    val csv = CSVWriter.open(out)(new DefaultCSVFormat {
      override val delimiter: Char = TextHiveDialect.delimiter
      override val lineTerminator: String = "\n"
    })

    override def write(row: InternalRow): Unit = {
      // builds a map of the column names to the row values (by using the source schema), then generates
      // a new sequence of values ordered by the columns in the target schema
      val map = sourceSchema.columnNames.zip(row).map { case (columName, value) => columName -> value }.toMap
      val seq = targetSchema.columnNames.map(map.apply)
      csv.writeRow(seq)
    }

    override def close(): Unit = {
      csv.close()
      out.close()
    }
  }
}
