package io.eels.component.avro

import java.nio.file.Path

import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Part, Row, Source}
import io.reactivex.functions.Consumer
import io.reactivex.{Emitter, Flowable}

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

class AvroSourcePart(val path: Path, val schema: StructType) extends Part {

  override def data(): Flowable[Row] = Flowable.generate(new Consumer[Emitter[Row]] {

    val deserializer = new AvroDeserializer()
    val reader = AvroReaderFns.createAvroReader(path)

    override def accept(e: Emitter[Row]): Unit = {
      try {
        if (reader.hasNext) {
          val record = reader.next()
          val row = deserializer.toRow(record)
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