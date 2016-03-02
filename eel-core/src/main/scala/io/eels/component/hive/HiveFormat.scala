package io.eels.component.hive

import io.eels.component.hive.dialect.{AvroHiveDialect, OrcHiveDialect, ParquetHiveDialect, TextHiveDialect}

trait HiveFormat {
  def serdeClass: String
  def inputFormatClass: String
  def outputFormatClass: String
}

object HiveFormat {

  def apply(format: String): HiveDialect = format match {
    case "avro" => AvroHiveDialect
    case "orc" => OrcHiveDialect
    case "parquet" => ParquetHiveDialect
    case "text" => TextHiveDialect
    case other => sys.error("Unknown hive input format: " + other)
  }

  case object Text extends HiveFormat {
    override def serdeClass: String = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    override def inputFormatClass: String = "org.apache.hadoop.mapred.TextInputFormat"
    override def outputFormatClass: String = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  }

  case object Parquet extends HiveFormat {
    override def serdeClass: String = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    override def inputFormatClass: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    override def outputFormatClass: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  }

  case object Avro extends HiveFormat {
    override def serdeClass: String = "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
    override def inputFormatClass: String = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"
    override def outputFormatClass: String = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"
  }

  case object Orc extends HiveFormat {
    override def serdeClass: String = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    override def inputFormatClass: String = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
    override def outputFormatClass: String = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
  }
}
