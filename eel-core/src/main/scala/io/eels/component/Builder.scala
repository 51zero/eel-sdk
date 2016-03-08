package io.eels.component

trait Builder[+T] {
  def apply(): T
}
