package io.eels.component.avro

import java.nio.file.Path
import java.util.function.Consumer

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Part, Row, Source}
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.control.NonFatal

case class AvroSource(path: Path) extends Source with Using {

  override def schema(): StructType = {
    using(AvroReaderFns.createAvroReader(path)) { reader =>
      val record = reader.next()
      AvroSchemaFns.fromAvroSchema(record.getSchema)
    }
  }

  override def parts(): List[Part] = List(new AvroSourcePart(path, schema()))
}

class AvroSourcePart(val path: Path, val schema: StructType) extends Part with Logging {

  override def data(): Flux[Row] = Flux.create(new Consumer[FluxSink[Row]] {
    override def accept(sink: FluxSink[Row]): Unit = {

      val deserializer = new AvroDeserializer()
      val reader = AvroReaderFns.createAvroReader(path)

      try {
        while (!sink.isCancelled && reader.hasNext) {
          val record = reader.next()
          val row = deserializer.toRow(record)
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