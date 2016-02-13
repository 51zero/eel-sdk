package io.eels.component.hive

trait HiveFormat {
  def serdeClass: String
  def outputFormatClass: String
  def inputFormatClass: String
}
object HiveFormat {
  case object Text extends HiveFormat {
    override def inputFormatClass: String = "org.apache.hadoop.mapred.TextInputFormat"
    override def outputFormatClass: String = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    override def serdeClass: String = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
  }
  case object Parquet extends HiveFormat {
    override def inputFormatClass: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    override def outputFormatClass: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    override def serdeClass: String = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  }
}
