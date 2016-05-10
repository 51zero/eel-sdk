package io.eels.component.avro

import io.eels.Column
import io.eels.Row
import io.eels.Schema
import io.eels.Source
import io.eels.component.Part
import io.eels.component.SourceReader
import java.nio.file.Path

import io.eels.component.Using

class AvroSource(val path: Path) : Source, Using {

  override fun schema(): Schema {
    return using(AvroReaderSupport.createReader(path), { reader ->
      val record = reader.next()
      val columns = record.schema.fields.map { it.name() }.map { Column(it) }
      // todo this should also take into account the field types
      Schema(columns)
    })
  }

  override fun parts(): List<Part> {
    val part = object : Part {
      override fun reader(): SourceReader = AvroSourceReader(path, schema())
    }
    return listOf(part)
  }
}

class AvroSourceReader(path: Path, schema: Schema) : SourceReader {

  private val reader = AvroReaderSupport.createReader(path)

  override fun close(): Unit = reader.close()
  override fun iterator(): Iterator<Row> = object : Iterator<Row> {
    override fun hasNext(): Boolean {
      val hasNext = reader.hasNext()
      if (!hasNext)
        reader.close()
      return hasNext
    }

    override fun next(): Row = avroRecordToRow(reader.next())
  }
}
