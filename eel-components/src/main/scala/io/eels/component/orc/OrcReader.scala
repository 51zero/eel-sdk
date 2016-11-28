package io.eels.component.orc

import java.util.function.Consumer

import com.sksamuel.exts.Logging
import io.eels.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.orc.OrcFile
import org.apache.orc.OrcFile.ReaderOptions
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.control.NonFatal

class OrcReader(path: Path)(implicit conf: Configuration) extends Logging {

  def rows(): Flux[Row] = Flux.create(new Consumer[FluxSink[Row]] {
    override def accept(sink: FluxSink[Row]): Unit = {

      val reader = OrcFile.createReader(path, new ReaderOptions(conf))
      val schema = OrcFns.readSchema(reader.getSchema)
      val batch = reader.getSchema().createRowBatch()
      val rows = reader.rows()

      try {
        while (!sink.isCancelled && rows.nextBatch(batch)) {
          val cols = batch.cols.map(_.asInstanceOf[BytesColumnVector])
          for (k <- 0 until batch.size) {
            val values = cols.map { col =>
              val bytes = col.vector.head.slice(col.start(k), col.start(k) + col.length(k))
              new String(bytes, "UTF8")
            }
            val row = Row(schema, values.toVector)
            sink.next(row)
          }
        }
        sink.complete()
      } catch {
        case NonFatal(error) =>
          logger.warn("Could not read file", error)
          sink.error(error)
      }
    }
  }, FluxSink.OverflowStrategy.BUFFER)
}
