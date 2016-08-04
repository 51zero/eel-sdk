package io.eels.component.sequence

import io.eels.Row
import io.eels.Source
import io.eels.component.Part
import io.eels.component.Using
import io.eels.schema.Schema
import io.eels.util.Logging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.IntWritable
import rx.Observable

class SequenceSource(val path: Path) : Source, Using, Logging {

  init {
    logger.debug("Creating sequence source from $path")
  }

  override fun schema(): Schema = SequenceSupport.schema(path)
  override fun parts(): List<Part> = listOf(SequencePart(path))

  class SequencePart(val path: Path) : Part {

    override fun data(): Observable<Row> {

      val reader = SequenceSupport.createReader(path)
      val k = IntWritable()
      val v = BytesWritable()
      val schema = SequenceSupport.schema(path)

      return Observable.create {
        it.onStart()
        // throw away top row as that's header
        reader.next(k, v)
        while (reader.next(k, v)) {
          val row = Row(schema, SequenceSupport.toValues(v).asList())
          it.onNext(row)
        }
        it.onCompleted()
      }
    }
  }
}