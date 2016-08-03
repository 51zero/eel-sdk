package io.eels.component.hive

import io.eels.Row
import io.eels.schema.Schema
import io.eels.component.Predicate
import io.eels.component.hive.dialect.ParquetHiveDialect
import io.eels.util.Logging
import io.eels.util.Option
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.Table
import rx.Observable

interface HiveDialect : Logging {

  /**
   * Creates an rows that will read from the given hadoop path.
   *
   * @path where to load the data from
   *
   * @metastoreSchema the schema as present in the metastore and used to match up with the raw data in dialects
   * where the schema is not present. For example with a CSV format in Hive, the metastoreSchema is required
   * in order to know what each column represents. We can't use the projection schema for this because the projection
   * schema might be in a different order.
   *
   * @projectionSchema the schema required to read. This might not be the full schema present in the data
   * but is required here because some file schemas can read data more efficiently
   * if they know they can omit some fields (eg Parquet).
   *
   * @predicate used by some implementations to filter data at a file read level (eg Parquet)
   *
   * The dataSchema represents the schema that was written for the data files. This won't necessarily be the same
   * as the hive metastore schema, because partition values are not written to the data files. We must include
   * this here because some hive formats don't store schema information with the data, eg delimited files.
   *
   * The readerSchema is the schema required by the caller which may be the same as the written data, or
   * it may be a subset if a projection pushdown is being used.
   */
  fun read(path: Path,
           metastoreSchema: Schema,
           projectionSchema: Schema,
           predicate: Option<Predicate>,
           fs: FileSystem): Observable<Row>

  fun writer(schema: Schema, path: Path, fs: FileSystem): HiveWriter

  companion object : Logging {

    operator fun invoke(format: String): HiveDialect = when (format) {
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" -> ParquetHiveDialect
//      "org.apache.hadoop.mapred.TextInputFormat" -> TextHiveDialect
//      "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" -> AvroHiveDialect
//      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" -> OrcHiveDialect
      else -> throw UnsupportedOperationException("Unknown hive input format $format")
    }

    operator fun invoke(table: Table): HiveDialect {
      val format = table.sd.inputFormat
      logger.debug("Table format is $format")
      val dialect = HiveDialect(format)
      logger.debug("HiveDialect is $dialect")
      return dialect
    }
  }
}