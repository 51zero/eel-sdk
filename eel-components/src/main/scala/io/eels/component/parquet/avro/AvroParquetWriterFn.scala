package io.eels.component.parquet.avro

import com.sksamuel.exts.Logging
import io.eels.component.parquet.ParquetWriterConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}

/**
  * Helper function for create an AvroParquetWriter using the apache parquet library.
  * Uses config keys to support compression codec, page size, and block size.
  *
  * @param path   the path to save the file at
  * @param schema the schema to use for this file. This schema is the one that will be set, regardless
  *               of the schema set in the data.
  */
object AvroParquetWriterFn extends Logging {
  def apply(path: Path, avroSchema: Schema): ParquetWriter[GenericRecord] = {
    val config = ParquetWriterConfig()
    AvroParquetWriter.builder[GenericRecord](path)
      .withSchema(avroSchema)
      .withCompressionCodec(config.compressionCodec)
      .withPageSize(config.pageSize)
      .withRowGroupSize(config.blockSize)
      .withDictionaryEncoding(config.enableDictionary)
      .withWriteMode(ParquetFileWriter.Mode.CREATE)
      .withValidation(config.validating)
      .build()
  }
}