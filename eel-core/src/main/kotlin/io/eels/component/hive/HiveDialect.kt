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
   * Creates a new reader that will read from the given hadoop path.
   *
   * The dataSchema represents the schema that was written for the data files. This won't necessarily be the same
   * as the hive metastore schema, because partition values are not written to the data files. We must include
   * this here because some hive formats don't store schema information with the data, eg delimited files.
   *
   * The projection is the schema required by the user which may be the same as the written data, or
   * it may be a subset if a projection pushdown is being used.
   *
   */
  fun reader(path: Path,
             dataSchema: Schema,
             projection: Schema,
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