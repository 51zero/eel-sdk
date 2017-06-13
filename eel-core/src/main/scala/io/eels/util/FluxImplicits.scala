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
  def filter(f: T => Boolean): Flux[T] = flux.filter(new function.Predicate[T] {
    override def test(t: T): Boolean = f(t)
  })
  def takeWhile(f: T => Boolean): Flux[T] = flux.takeWhile(new function.Predicate[T] {
    override def test(t: T): Boolean = f(t)
  })
  def takeUntil(f: T => Boolean): Flux[T] = flux.takeUntil(new function.Predicate[T] {
    override def test(t: T): Boolean = f(t)
  })
  def asJava: Flux[T] = flux
}
