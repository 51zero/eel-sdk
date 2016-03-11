package io.eels.component.avro

import io.eels.InternalRow
import org.apache.avro.generic.GenericRecord

trait AvroRecordMarshaller {
  def toRecord(row: InternalRow): GenericRecord
}
