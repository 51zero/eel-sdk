package io.eels.component

trait SourceBuilder[+T] {
  def apply: T
}
