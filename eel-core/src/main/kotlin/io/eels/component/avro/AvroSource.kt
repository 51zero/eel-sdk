package io.eels.component.avro

import io.eels.schema.Field
import io.eels.Row
import io.eels.schema.Schema
import io.eels.Source
import io.eels.component.Part
import java.nio.file.Path

import io.eels.component.Using
import rx.Observable

class AvroSource(val path: Path) : Source, Using {

  override fun schema(): Schema {
    return using(AvroReaderSupport.createReader(path), { reader ->
      val record = reader.next()
      val columns = record.schema.fields.map { it.name() }.map { Field(it) }
      // todo this should also take into account the field types
      Schema(columns)
    })
  }

  override fun parts(): List<Part> = listOf(AvroSourcePart(path, schema()))
}

class AvroSourcePart(val path: Path, val schema: Schema) : Part {

  override fun data(): Observable<Row> = Observable.create<Row> {

    val reader = AvroReaderSupport.createReader(path)
    it.onStart()

    while (reader.hasNext() && !it.isUnsubscribed) {
      val record = reader.next()
      val row = avroRecordToRow(record)
      it.onNext(row)
    }

    if (!it.isUnsubscribed)
      it.onCompleted()
  }
}