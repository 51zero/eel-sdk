package io.eels.component.avro

import java.nio.file.Path

import com.sksamuel.exts.io.Using
import io.eels.schema.Schema
import io.eels.{Part, Row, Source}
import rx.lang.scala._

class AvroSource(val path: Path) extends Source with Using {

  override def schema(): Schema = {
    using(AvroReaderFns.createAvroReader(path)) { reader =>
      val record = reader.next()
      AvroSchemaFns.fromAvroSchema(record.getSchema)
    }
  }

  override def parts(): List[Part] = List(new AvroSourcePart(path, schema()))
}

class AvroSourcePart(val path: Path, val schema: Schema) extends Part {

  override def data(): Observable[Row] = Observable.apply { subscriber =>

    val deserializer = new AvroRecordDeserializer()
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