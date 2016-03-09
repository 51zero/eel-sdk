package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{InternalRow, Schema, SourceReader}
import io.eels.component.hive.dialect.{AvroHiveDialect, OrcHiveDialect, ParquetHiveDialect, TextHiveDialect}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.api.Table

trait HiveDialect extends StrictLogging {

  def reader(path: Path, schema: Schema, columns: Seq[String])
            (implicit fs: FileSystem): SourceReader

  def writer(schema: Schema, path: Path)
            (implicit fs: FileSystem): HiveWriter
}

object HiveDialect extends Logging {

  def apply(table: Table): HiveDialect = {
    val format = table.getSd.getInputFormat
    logger.debug(s"Table format is $format")
    val dialect = HiveDialect(format)
    logger.debug(s"HiveDialect is $dialect")
    dialect
  }

  def apply(format: String): HiveDialect = format match {
    case "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" => ParquetHiveDialect
    case "org.apache.hadoop.mapred.TextInputFormat" => TextHiveDialect
    case "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" => AvroHiveDialect
    case "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" => OrcHiveDialect
    case other => sys.error("Unknown hive input format: " + other)
  }
}

trait HiveWriter {
  def write(row: InternalRow): Unit
  def close(): Unit
}