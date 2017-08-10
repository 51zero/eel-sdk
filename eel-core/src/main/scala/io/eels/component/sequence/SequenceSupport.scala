package io.eels.component.sequence

import java.io.StringReader
import java.nio.charset.Charset

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.component.csv.{CsvFormat, CsvSupport}
import io.eels.schema.{Field, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}

object SequenceSupport extends Logging with Using {

  def createReader(path: Path)(implicit conf: Configuration): SequenceFile.Reader =
    new SequenceFile.Reader(conf, SequenceFile.Reader.file(path))

  def toValues(v: BytesWritable): Vector[String] = toValues(new String(v.copyBytes(), Charset.forName("UTF8")))

  def toValues(str: String): Vector[String] = {
    val parser = CsvSupport.createParser(CsvFormat(), false, false, false, null, null)
    parser.beginParsing(new StringReader(str))
    val record = parser.parseNext()
    parser.stopParsing()
    record.toVector
  }

  def schema(path: Path)(implicit conf: Configuration): StructType = {
    logger.debug(s"Fetching sequence schema for $path")
    using(createReader(path)) { it =>
      val k = new IntWritable()
      val v = new BytesWritable()
      val fields: Vector[Field] = {
        it.next(k, v)
        toValues(v).map { it => new Field(it) }
      }
      StructType(fields.toList)
    }
  }
}