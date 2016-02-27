package io.eels.component.hive

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{InternalRow, Schema}
import io.eels.component.hive.dialect.{AvroHiveDialect, OrcHiveDialect, ParquetHiveDialect, TextHiveDialect}
import org.apache.hadoop.fs.{FileSystem, Path}

trait HiveDialect extends StrictLogging {

  def iterator(path: Path, schema: Schema, columns: Seq[String])
              (implicit fs: FileSystem): Iterator[InternalRow]

  def writer(schema: Schema, path: Path)
            (implicit fs: FileSystem): HiveWriter
}

object HiveDialect {
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