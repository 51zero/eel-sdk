package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.{Predicate, Row}
import io.eels.schema.Schema
import io.eels.component.hive.dialect.ParquetHiveDialect
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.Table
import rx.lang.scala.Observable

trait HiveDialect extends Logging {

  /**
   * Creates an rows that will read from the given hadoop path.
   *
   * @param path where to load the data from
   *
   * @param metastoreSchema the schema as present in the metastore and used to match up with the raw data in dialects
   * where the schema is not present. For example with a CSV format in Hive, the metastoreSchema is required
   * in order to know what each column represents. We can't use the projection schema for this because the projection
   * schema might be in a different order.
   *
   * @param projectionSchema the schema required to read. This might not be the full schema present in the data
   * but is required here because some file schemas can read data more efficiently
   * if they know they can omit some fields (eg Parquet).
   *
   * @param predicate used by some implementations to filter data at a file read level (eg Parquet)
   *
   * The dataSchema represents the schema that was written for the data files. This won't necessarily be the same
   * as the hive metastore schema, because partition values are not written to the data files. We must include
   * this here because some hive formats don't store schema information with the data, eg delimited files.
   *
   * The readerSchema is the schema required by the caller which may be the same as the written data, or
   * it may be a subset if a projection pushdown is being used.
   */
  def read(path: Path,
           metastoreSchema: Schema,
           projectionSchema: Schema,
           predicate: Option[Predicate])
          (implicit fs: FileSystem): Observable[Row]

  def writer(schema: Schema, path: Path)
            (implicit fs: FileSystem): HiveWriter
}

object HiveDialect extends Logging {

  def apply(format: String): HiveDialect = format match {
    case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" => ParquetHiveDialect
    //      "org.apache.hadoop.mapred.TextInputFormat" -> TextHiveDialect
    //      "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" -> AvroHiveDialect
    //      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" -> OrcHiveDialect
    case _ => throw new UnsupportedOperationException("Unknown hive input format $format")
  }

  def apply(table: Table): HiveDialect = {
    val format = table.getSd.getInputFormat
    logger.debug("Table format is $format")
    val dialect = HiveDialect(format)
    logger.debug("HiveDialect is $dialect")
    dialect
  }
}