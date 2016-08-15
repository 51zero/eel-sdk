package io.eels.component.sequence

import au.com.bytecode.opencsv.CSVReader
import io.eels.schema.Field
import io.eels.schema.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, SequenceFile}
import java.io.StringReader
import java.nio.charset.Charset

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using

object SequenceSupport extends Logging with Using {

  def createReader(path: Path): SequenceFile.Reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(path))

  def toValues(v: BytesWritable): Array[String] = toValues(new String(v.copyBytes(), Charset.forName("UTF8")))

  def toValues(str: String): Array[String] = {
    val csv = new CSVReader(new StringReader(str))
    val row = csv.readNext()
    csv.close()
    row
  }

  def schema(path: Path): Schema = {
    logger.debug(s"Fetching sequence schema for $path")
    using(createReader(path)) { it =>
      val k = new IntWritable()
      val v = new BytesWritable()
      val fields: Array[Field] = {
        it.next(k, v)
        toValues(v).map { it => new Field(it) }
      }
      Schema(fields.toList)
    }
  }
}