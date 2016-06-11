package io.eels.component.hive

interface HiveFormat {

  fun serdeClass(): String
  fun inputFormatClass(): String
  fun outputFormatClass(): String

  companion object {

    fun apply(format: String): HiveFormat = when (format) {
      "avro" -> HiveFormat.Avro
      "orc" -> HiveFormat.Orc
      "parquet" -> HiveFormat.Parquet
      "text" -> HiveFormat.Text
      else -> throw UnsupportedOperationException("Unknown hive input format $format")
    }

    fun fromInputFormat(inputFormat: String): HiveFormat = when (inputFormat) {
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat" -> Parquet
      "org.apache.hadoop.mapred.TextInputFormat" -> Text
      "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat" -> Avro
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" -> Orc
      else -> throw UnsupportedOperationException("Input format not known $inputFormat")
    }
  }

  object Text : HiveFormat {
    override fun serdeClass(): String = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    override fun inputFormatClass(): String = "org.apache.hadoop.mapred.TextInputFormat"
    override fun outputFormatClass(): String = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
  }

  object Parquet : HiveFormat {
    override fun serdeClass(): String = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    override fun inputFormatClass(): String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    override fun outputFormatClass(): String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  }

  object Avro : HiveFormat {
    override fun serdeClass(): String = "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
    override fun inputFormatClass(): String = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"
    override fun outputFormatClass(): String = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"
  }

  object Orc : HiveFormat {
    override fun serdeClass(): String = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    override fun inputFormatClass(): String = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
    override fun outputFormatClass(): String = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
  }
}