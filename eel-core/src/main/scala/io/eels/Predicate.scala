package io.eels

import io.eels.coercion.{DoubleCoercer, FloatCoercer, IntCoercer, LongCoercer}

sealed trait Predicate {
  // returns a list of fields that this predicate operates on
  def fields(): Seq[String]
  // apply the predicate to this row and return true if it matches
  def apply(row: Row): Boolean
}

abstract class NamedPredicate(name: String) extends Predicate {
  override def fields(): Seq[String] = Seq(name)
}

abstract case class NotPredicate(predicate: Predicate) extends Predicate {
  override def fields(): Seq[String] = predicate.fields()
}

case class OrPredicate(predicates: Seq[Predicate]) extends Predicate {
  override def fields(): Seq[String] = predicates.flatMap(_.fields)
  override def apply(row: Row): Boolean = predicates.exists(_.apply(row))
}

case class AndPredicate(predicates: Seq[Predicate]) extends Predicate {
  override def fields(): Seq[String] = predicates.flatMap(_.fields)
  override def apply(row: Row): Boolean = predicates.forall(_.apply(row))
}

case class EqualsPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def apply(row: Row): Boolean = row.get(name) == value
}

case class NotEqualsPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def apply(row: Row): Boolean = row.get(name) == value
}

case class LtPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def apply(row: Row): Boolean = row.get(name) match {
    case long: Long => long < LongCoercer.coerce(value)
    case double: Double => double < DoubleCoercer.coerce(value)
    case float: Float => float < FloatCoercer.coerce(value)
    case int: Int => int < IntCoercer.coerce(value)
  }
}

case class LtePredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def apply(row: Row): Boolean = row.get(name) match {
    case long: Long => long <= LongCoercer.coerce(value)
    case double: Double => double <= DoubleCoercer.coerce(value)
    case float: Float => float <= FloatCoercer.coerce(value)
    case int: Int => int <= IntCoercer.coerce(value)
  }
}

case class GtPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def apply(row: Row): Boolean = row.get(name) match {
    case long: Long => long > LongCoercer.coerce(value)
    case double: Double => double > DoubleCoercer.coerce(value)
    case float: Float => float > FloatCoercer.coerce(value)
    case int: Int => int > IntCoercer.coerce(value)
  }
}

case class GtePredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def apply(row: Row): Boolean = row.get(name) match {
    case long: Long => long >= LongCoercer.coerce(value)
    case double: Double => double >= DoubleCoercer.coerce(value)
    case float: Float => float >= FloatCoercer.coerce(value)
    case int: Int => int >= IntCoercer.coerce(value)
  }
}

object Predicate {

  def or(left: Predicate, right: Predicate) = OrPredicate(Seq(left, right))
  def and(left: Predicate, right: Predicate) = AndPredicate(Seq(left, right))
  def equals(name: String, value: Any) = EqualsPredicate(name, value)
  def gt(name: String, value: Any) = GtPredicate(name, value)
  def gte(name: String, value: Any) = GtePredicate(name, value)
  def lt(name: String, value: Any) = LtPredicate(name, value)
  def lte(name: String, value: Any) = LtePredicate(name, value)
}