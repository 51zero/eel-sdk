package io.eels

import io.eels.coercion.{DoubleCoercer, FloatCoercer, IntCoercer, LongCoercer}
import io.eels.schema.DataType

sealed trait Predicate {
  // returns a list of fields that this predicate operates on
  def fields(): Seq[String]
  // apply the predicate to this row and return true if it matches
  @deprecated("will move this functionality into an 'eel predicate builder' like the 'parquet predicate builder'", "1.2.0")
  def eval(row: Row): Boolean
}

trait UserDefinedPredicate[T] extends Predicate with Serializable {
  // the field name to evaluate the value against
  def name: String
  // the type of the field to operate on, this must be compatible with the type in the group stats
  def datatype: DataType
  // can we drop a group of rows, for when this predicate is being evaluated
  def canDropGroup(stats: GroupStats[T]): Boolean
  // can we drop a group of rows when this predicate is inversed - ie, it is in a not predicate
  def inverseCanDropGroup(stats: GroupStats[T]): Boolean
  def keep(value: T): Boolean
}

case class GroupStats[T](min: T, max: T)

// superclass for those predicates which have a field name applied
abstract class NamedPredicate(name: String) extends Predicate {
  override def fields(): Seq[String] = Seq(name)
}

// reverses the predicate
case class NotPredicate(inner: Predicate) extends Predicate {
  override def fields(): Seq[String] = inner.fields()
  override def eval(row: Row): Boolean = !inner.eval(row)
}

case class OrPredicate(predicates: Seq[Predicate]) extends Predicate {
  override def fields(): Seq[String] = predicates.flatMap(_.fields)
  override def eval(row: Row): Boolean = predicates.exists(_.eval(row))
}

case class AndPredicate(predicates: Seq[Predicate]) extends Predicate {
  override def fields(): Seq[String] = predicates.flatMap(_.fields)
  override def eval(row: Row): Boolean = predicates.forall(_.eval(row))
}

case class EqualsPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = row.get(name) == value
}

case class NotEqualsPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = row.get(name) == value
}

case class LtPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = row.get(name) match {
    case long: Long => long < LongCoercer.coerce(value)
    case double: Double => double < DoubleCoercer.coerce(value)
    case float: Float => float < FloatCoercer.coerce(value)
    case int: Int => int < IntCoercer.coerce(value)
  }
}

case class LtePredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = row.get(name) match {
    case long: Long => long <= LongCoercer.coerce(value)
    case double: Double => double <= DoubleCoercer.coerce(value)
    case float: Float => float <= FloatCoercer.coerce(value)
    case int: Int => int <= IntCoercer.coerce(value)
  }
}

case class GtPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = row.get(name) match {
    case long: Long => long > LongCoercer.coerce(value)
    case double: Double => double > DoubleCoercer.coerce(value)
    case float: Float => float > FloatCoercer.coerce(value)
    case int: Int => int > IntCoercer.coerce(value)
  }
}

case class GtePredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = row.get(name) match {
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