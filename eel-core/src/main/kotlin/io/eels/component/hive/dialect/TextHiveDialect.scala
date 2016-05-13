package io.eels.component.hive.dialect

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.github.tototoshi.csv.{CSVWriter, DefaultCSVFormat}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.hive.{HiveDialect, HiveWriter, Predicate}
import io.eels.{InternalRow, Schema, SourceReader}
import org.apache.hadoop.fs.{FileSystem, Path}

object TextHiveDialect extends HiveDialect with StrictLogging {

  val delimiter = '\u0001'

  private def lineIterator(in: InputStream): Iterator[String] = {
    val buff = new BufferedReader(new InputStreamReader(in))
    Iterator.continually(buff.readLine).takeWhile(_ != null)
  }

  // todo add case insensitivity
  override def writer(schema: Schema, path: Path)
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
      val map = schema.columnNames.zip(row).map { case (columName, value) => columName -> value }.toMap
      val seq = schema.columnNames.map(map.apply)
      csv.writeRow(seq)
    }

    override def close(): Unit = {
      csv.close()
      out.close()
    }
  }

  override def reader(path: Path, tableSchema: Schema, requestedSchema: Schema, predicate: Option[Predicate])
                     (implicit fs: FileSystem): SourceReader = new SourceReader {
    val in = fs.open(path)
    val iter = lineIterator(in)
    override def close(): Unit = in.close()
    override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): InternalRow = iter.next.split(delimiter).padTo(tableSchema.size, null).toSeq
    }
  }
}
