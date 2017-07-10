package io.eels.component

import io.eels.{Sink, Source}

trait Component {
  def source(): Source
  def sink(): Sink
}