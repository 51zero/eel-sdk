package io.eels.component.sequence

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Part, Row, Source}
import io.reactivex.functions.Consumer
import io.reactivex.{Emitter, Flowable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable}

import scala.util.control.NonFatal

case class SequenceSource(path: Path)(implicit conf: Configuration) extends Source with Using with Logging {
    logger.debug("Creating sequence source from $path")

  override def schema(): StructType = SequenceSupport.schema(path)
  override def parts(): List[Part] = List(new SequencePart(path))
}

class SequencePart(val path: Path)(implicit conf: Configuration) extends Part {

  override def data(): Flowable[Row] = Flowable.generate(new Consumer[Emitter[Row]] {

    val reader = SequenceSupport.createReader(path)
    val k = new IntWritable()
    val v = new BytesWritable()
    val schema = SequenceSupport.schema(path)

    // throw away top row as that's header
    reader.next(k, v)

    override def accept(e: Emitter[Row]): Unit = {
      try {
        if (reader.next(k, v)) {
          val row = Row(schema, SequenceSupport.toValues(v).toVector)
          e.onNext(row)
        } else {
          e.onComplete()
        }
      } catch {
        case NonFatal(t) => e.onError(t)
      } finally {
        reader.close()
      }
    }
  })
}