package io.eels.component.sequence

import java.util.function.Consumer

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Part, Row, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.control.NonFatal

case class SequenceSource(path: Path)(implicit conf: Configuration) extends Source with Using with Logging {
    logger.debug("Creating sequence source from $path")

  override def schema(): StructType = SequenceSupport.schema(path)
  override def parts(): List[Part] = List(new SequencePart(path))
}

class SequencePart(val path: Path)(implicit conf: Configuration) extends Part with Logging {

  override def data(): Flux[Row] = Flux.create(new Consumer[FluxSink[Row]] {
    override def accept(sink: FluxSink[Row]): Unit = {

      val reader = SequenceSupport.createReader(path)
      val k = new IntWritable()
      val v = new BytesWritable()
      val schema = SequenceSupport.schema(path)

      // throw away top row as that's header
      reader.next(k, v)

      try {
        while (!sink.isCancelled && reader.next(k, v)) {
          val row = Row(schema, SequenceSupport.toValues(v).toVector)
          sink.next(row)
        }
        sink.complete()
      } catch {
        case NonFatal(error) =>
          logger.warn("Could not read file", error)
          sink.error(error)
      } finally {
        reader.close()
      }
    }
  }, FluxSink.OverflowStrategy.BUFFER)
}