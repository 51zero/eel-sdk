package io.eels.component.parquet

import io.eels.{FilePattern, Source}
import io.eels.component.Component

class ParquetComponent extends Component {
  override def source(path: String, params: Map[String, String]): Source = {
    val pattern = FilePattern(path)
    ParquetSource(pattern)
  }
}