package io.eels.component.hive

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.avro.AvroSchemaGen
import io.eels.component.parquet.ParquetIterator
import io.eels.{Field, Row, FrameSchema}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter

object ParquetHiveDialect extends HiveDialect with StrictLogging {

  override def iterator(path: Path, schema: FrameSchema)
                       (implicit fs: FileSystem): Iterator[Row] = new Iterator[Row] {
    lazy val iter = ParquetIterator(path)
    override def hasNext: Boolean = iter.hasNext
    override def next(): Row = {
      val map = iter.next.toMap
      val fields = for ( column <- schema.columns ) yield Field(map.getOrElse(column.name, null))
      Row(schema.columns, fields)
    }
  }

  override def writer(schema: FrameSchema, path: Path)
                     (implicit fs: FileSystem): HiveWriter = {
    logger.debug(s"Creating parquet writer for $path")
    val avroSchema = AvroSchemaGen(schema)
    val writer = new AvroParquetWriter[GenericRecord](path, avroSchema)
    new HiveWriter {
      override def close(): Unit = writer.close()
      override def write(row: Row): Unit = {
        val record = new Record(avroSchema)
        for ( (key, value) <- row.toMap ) {
          record.put(key, value)
        }
        writer.write(record)
      }
    }
  }
}
