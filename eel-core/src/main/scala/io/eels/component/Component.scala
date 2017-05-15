package io.eels.component

import io.eels.Source

trait Component {
  // Creates a new Source using the parameter map to set parameters
  def source(params: Map[String, String]): Source
}
