package io.eels.component.sequence

import au.com.bytecode.opencsv.CSVReader
import io.eels.schema.Field
import io.eels.util.Logging
import io.eels.Row
import io.eels.schema.Schema
import io.eels.Source
import io.eels.component.Part
import io.eels.component.Using
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile
import rx.Observable
import java.io.StringReader

class SequenceSource(val path: Path) : Source, Using, Logging, SequenceSupport {

  init {
    logger.debug("Creating sequence source from $path")
  }

  override fun schema(): Schema = schema(path)
  override fun parts(): List<Part> = listOf(SequencePart(path))
}

interface SequenceSupport : Logging, Using {

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

class SequencePart(val path: Path) : Part, SequenceSupport {

  override fun data(): Observable<Row> {

    val reader = createReader(path)
    val k = IntWritable()
    val v = BytesWritable()
    val schema = schema(path)

    return Observable.create {
      it.onStart()
      while (reader.next(k, v)) {
        val row = Row(schema, toValues(v).asList())
        it.onNext(row)
      }
      it.onCompleted()
    }
  }
}