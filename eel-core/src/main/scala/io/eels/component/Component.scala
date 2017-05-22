package io.eels.component

import io.eels.Source

trait Component {
  // Creates a new Source using the parameter map to set parameters
  def source(path: String, params: Map[String, String], registry: EelRegistry): Source
}
