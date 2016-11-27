package io.eels.component.avro

import java.nio.file.Path

import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Part, Row, Source}
import rx.lang.scala._

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

  override def data(): Observable[Row] = Observable.apply { subscriber =>

    val deserializer = new AvroDeserializer()
    val reader = AvroReaderFns.createAvroReader(path)
    subscriber.onStart()

    while (reader.hasNext() && !subscriber.isUnsubscribed) {
      val record = reader.next()
      val row = deserializer.toRow(record)
      subscriber.onNext(row)
    }

    if (!subscriber.isUnsubscribed)
      subscriber.onCompleted()

    reader.close()
  }
}