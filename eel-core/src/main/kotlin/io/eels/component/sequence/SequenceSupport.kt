package io.eels.component.sequence

import au.com.bytecode.opencsv.CSVReader
import io.eels.component.Using
import io.eels.schema.Field
import io.eels.schema.Schema
import io.eels.util.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile
import java.io.StringReader

object SequenceSupport : Logging, Using {

  fun createReader(path: Path): SequenceFile.Reader = SequenceFile.Reader(Configuration(), SequenceFile.Reader.file(path))

  fun toValues(v: BytesWritable): Array<String> = toValues(String(v.copyBytes(), charset("UTF8")))

  fun toValues(str: String): Array<String> {
    val csv = CSVReader(StringReader(str))
    val row = csv.readNext()
    csv.close()
    return row
  }

  fun schema(path: Path): Schema {
    logger.debug("Fetching sequence schema for $path")
    return using(createReader(path)) {
      val k = IntWritable()
      val v = BytesWritable()
      val fields: List<Field> = {
        it.next(k, v)
        toValues(v).map { Field(it) }
      }()
      Schema(fields)
    }
  }
}