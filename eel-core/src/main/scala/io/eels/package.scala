package io

import io.eels.Row
import io.eels.coercion.BigDecimalCoercer

package object eels {
  def select(name: String): Select = Select(name)
}

case class Select(name: String) {
  def *(long: Long): Multiply = Multiply(this, LongLiteral(long))
  def *(double: Double): Multiply = Multiply(this, DoubleLiteral(double))
  def *(bigd: BigDecimal): Multiply = Multiply(this, BigDecimalLiteral(bigd))

  def +(long: Long): Addition = Addition(this, LongLiteral(long))
  def +(double: Double): Addition = Addition(this, DoubleLiteral(double))
  def +(bigd: BigDecimal): Addition = Addition(this, BigDecimalLiteral(bigd))

  def -(long: Long): Subtraction = Subtraction(this, LongLiteral(long))
  def -(double: Double): Subtraction = Subtraction(this, DoubleLiteral(double))
  def -(bigd: BigDecimal): Subtraction = Subtraction(this, BigDecimalLiteral(bigd))

  def /(long: Long): Division = Division(this, LongLiteral(long))
  def /(double: Double): Division = Division(this, DoubleLiteral(double))
  def /(bigd: BigDecimal): Division = Division(this, BigDecimalLiteral(bigd))

  def ===(value: Any): Equals = Equals(this, AnyLiteral(value))
}

trait Literal {
  def value: Any
}

case class BigDecimalLiteral(value: BigDecimal) extends Literal
case class LongLiteral(value: Long) extends Literal
case class DoubleLiteral(value: Double) extends Literal
case class AnyLiteral(value: Any) extends Literal

trait Expression {
  def evalulate(row: Row): Any
}

case class Equals(lhs: Select, rhs: Literal) extends Expression {
  override def evalulate(row: Row): Any = row(lhs.name) == rhs.value
}

case class Multiply(lhs: Select, rhs: Literal) extends Expression {
  override def evalulate(row: Row): Any = rhs match {
    case BigDecimalLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) * bigd
    case DoubleLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) * bigd
    case LongLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) * bigd
  }
}

case class Addition(lhs: Select, rhs: Literal) extends Expression {
  override def evalulate(row: Row): Any = rhs match {
    case BigDecimalLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) + bigd
    case DoubleLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) + bigd
    case LongLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) + bigd
  }
}

case class Subtraction(lhs: Select, rhs: Literal) extends Expression {
  override def evalulate(row: Row): Any = rhs match {
    case BigDecimalLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) - bigd
    case DoubleLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) - bigd
    case LongLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) - bigd
  }
}

case class Division(lhs: Select, rhs: Literal) extends Expression {
  override def evalulate(row: Row): Any = rhs match {
    case BigDecimalLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) / bigd
    case DoubleLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) / bigd
    case LongLiteral(bigd) => BigDecimalCoercer.coerce(row(lhs.name)) / bigd
  }
}