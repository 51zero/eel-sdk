package io.eels.component.avro

import java.nio.file.Path

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels._
import io.eels.schema.StructType

case class AvroSource(path: Path) extends Source with Using {

  override def schema(): StructType = {
    using(AvroReaderFns.createAvroReader(path)) { reader =>
      val record = reader.next()
      AvroSchemaFns.fromAvroSchema(record.getSchema)
    }
  }

  override def parts2(): List[Part2] = List(new AvroSourcePart(path, schema()))
}

class AvroSourcePart(val path: Path, val schema: StructType) extends Part2 with Logging {

  override def stream(): PartStream = new PartStream {

    val deserializer = new AvroDeserializer()
    val reader = AvroReaderFns.createAvroReader(path)

    val iter = AvroRecordIterator(reader).map { record =>
      deserializer.toRow(record)
    }.grouped(1000).withPartial(true)

    var closed = false

    override def next(): List[Row] = iter.next
    override def hasNext(): Boolean = !closed && iter.hasNext
    override def close(): Unit = {
      closed = true
      reader.close()
    }
  }
}