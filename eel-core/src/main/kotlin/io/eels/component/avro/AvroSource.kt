package io.eels.component.avro

import io.eels.Row
import io.eels.Source
import io.eels.component.Part
import io.eels.component.Using
import io.eels.schema.Schema
import rx.Observable
import java.nio.file.Path

class AvroSource(val path: Path) : Source, Using {

  override fun schema(): Schema {
    return using(createAvroReader(path), { reader ->
      val record = reader.next()
      fromAvroSchema(record.schema)
    })
  }

  override fun parts(): List<Part> = listOf(AvroSourcePart(path, schema()))
}

class AvroSourcePart(val path: Path, val schema: Schema) : Part {

  override fun data(): Observable<Row> = Observable.create<Row> { subscriber ->

    val reader = createAvroReader(path)
    subscriber.onStart()

    while (reader.hasNext() && !subscriber.isUnsubscribed) {
      val record = reader.next()
      val row = AvroRecordDeserializer().toRow(record)
      subscriber.onNext(row)
    }

    if (!subscriber.isUnsubscribed)
      subscriber.onCompleted()

    reader.close()
  }
}