package io.eels.component.hive

trait HiveFormat {
  def inputFormatClass: String
}
object HiveFormat {
  case object Text extends HiveFormat {
    override def inputFormatClass: String = "org.apache.hadoop.mapred.TextInputFormat"
  }
  case object Parquet extends HiveFormat {
    override def inputFormatClass: String = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  }
}
