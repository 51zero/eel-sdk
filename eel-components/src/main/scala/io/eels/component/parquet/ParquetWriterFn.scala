package io.eels.component.parquet

import io.eels.Row
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.schema.MessageType

/**
  * Helper function for create a native ParquetWriter using the apache parquet library.
  * Uses config keys to support compression codec, page size, and block size.
  *
  * @param path   the path to save the file at
  * @param schema the schema to use for this file. This schema is the one that will be set, regardless
  *               of the schema set in the data.
  */
object ParquetWriterFn {

  def apply(path: Path, schema: StructType): ParquetWriter[Row] = {
    val config = ParquetWriterConfig()
    val parquetSchema = ParquetSchemaFns.toParquetSchema(schema)
    new RowParquetWriterBuilder(path, parquetSchema)
      .withCompressionCodec(config.compressionCodec)
      .withDictionaryEncoding(config.enableDictionary)
      .withRowGroupSize(config.blockSize)
      .withPageSize(config.pageSize)
      .withWriteMode(ParquetFileWriter.Mode.CREATE)
      .withValidation(config.validating)
      .build()
  }
}

class RowParquetWriterBuilder(path: Path, schema: MessageType)
  extends ParquetWriter.Builder[Row, RowParquetWriterBuilder](path) {
  override def getWriteSupport(conf: Configuration): WriteSupport[Row] = new RowWriteSupport(schema)
  override def self(): RowParquetWriterBuilder = this
}