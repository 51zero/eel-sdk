package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.component.hive.dialect.ParquetHiveDialect
import io.eels.component.parquet.Predicate
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.api.Table

trait HiveDialect extends Logging {

  /**
   * Creates a closeable iterator that will read from the given hadoop path.
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
           metastoreSchema: StructType,
           projectionSchema: StructType,
           predicate: Option[Predicate])
          (implicit fs: FileSystem, conf: Configuration): CloseableIterator[List[Row]]

  def writer(schema: StructType,
             path: Path,
             permission: Option[FsPermission])
            (implicit fs: FileSystem, conf: Configuration): HiveWriter
}

object HiveDialect extends Logging {

  def apply(format: String): HiveDialect = format match {
    case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" => ParquetHiveDialect
//    case "org.apache.orc.mapreduce.OrcInputFormat" => OrcHiveDialect
//    case "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" => OrcHiveDialect
    //      "org.apache.hadoop.mapred.TextInputFormat" -> TextHiveDialect
    //      "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" -> AvroHiveDialect
    case _ => throw new UnsupportedOperationException(s"Unknown hive input format $format")
  }

  def apply(table: Table): HiveDialect = {
    val format = table.getSd.getInputFormat
    logger.debug(s"Table format is $format")
    val dialect = HiveDialect(format)
    logger.debug(s"HiveDialect is $dialect")
    dialect
  }
}