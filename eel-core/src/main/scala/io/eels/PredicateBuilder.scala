package io.eels

trait PredicateBuilder[T] {
  def build(predicate: Predicate): T
}
