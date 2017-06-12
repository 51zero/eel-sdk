package io.eels.util

import java.util.function

import reactor.core.publisher.Flux

object FluxImplicits {
  implicit class RichFlux[T](flux: Flux[T]) {
    def asScala = new ScalaFlux(flux)
  }
}

class ScalaFlux[T](flux: Flux[T]) {
  def map[U](f: T => U): Flux[U] = flux.map(new function.Function[T, U] {
    override def apply(t: T): U = f(t)
  })
  def asJava: Flux[T] = flux
}
