package io.eels.component.sequence

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Part, Row, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import rx.lang.scala.Observable

case class SequenceSource(path: Path)(implicit conf: Configuration) extends Source with Using with Logging {
    logger.debug("Creating sequence source from $path")

  override def schema(): StructType = SequenceSupport.schema(path)
  override def parts(): List[Part] = List(new SequencePart(path))

  class SequencePart(val path: Path) extends Part {

    override def data(): Observable[Row] = {

      val reader = SequenceSupport.createReader(path)
      val k = new IntWritable()
      val v = new BytesWritable()
      val schema = SequenceSupport.schema(path)

      Observable { it =>
        it.onStart()
        // throw away top row as that's header
        reader.next(k, v)
        while (reader.next(k, v)) {
          val row = Row(schema, SequenceSupport.toValues(v).toVector)
          it.onNext(row)
        }
        it.onCompleted()
      }
    }
  }
}