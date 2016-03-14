package io.eels.component.parquet

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{InternalRow, Schema}
import io.eels.component.avro.{AvroRecordFn, AvroSchemaFn}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader

object ParquetIterator extends StrictLogging {

  // creates a new parquet iterator. The schema must match the schema used in the reader
  def apply(reader: ParquetReader[GenericRecord], schema: Schema): Iterator[InternalRow] = new Iterator[InternalRow] {
    require(schema.columns.nonEmpty, "Attempted to create a parquet iterator with no schema columns")

    val recordFn = new AvroRecordFn
    val avroSchema = AvroSchemaFn.toAvro(schema)

    val iter = Iterator.continually(reader.read).takeWhile(_ != null).map { record =>
      val row = recordFn.fromRecord(record, avroSchema)
      require(row.size == schema.size, s"Row is missing values for some schema fields [row=$row; schema=$schema]")
      row
    }

    override def hasNext: Boolean = {
      val hasNext = iter.hasNext
      if (!hasNext) {
        reader.close()
      }
      hasNext
    }

    override def next(): InternalRow = iter.next()
  }
}

