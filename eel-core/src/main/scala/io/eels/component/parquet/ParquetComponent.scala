package io.eels.component.parquet

import io.eels.{FilePattern, Source}
import io.eels.component.{Component, EelRegistry}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

class ParquetComponent extends Component {
  override def source(path: String, params: Map[String, String], registry: EelRegistry): Source = {
    implicit val fs = registry.get[FileSystem]("fs")
    implicit val conf = registry.get[Configuration]("conf")
    val pattern = FilePattern(path)
    ParquetSource(pattern)
  }
}