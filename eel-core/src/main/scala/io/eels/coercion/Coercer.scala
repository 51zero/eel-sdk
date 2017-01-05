package io.eels.coercion

/**
  * A Coercer will coerce input values into an output value of type T.
  * The types of coercion supported depends on the implementation but for example,
  * a Double coercer might support converting a String, Float and Long to a Double.
  */
trait Coercer[T] {
  def coerce(input: Any): T
}

object LongCoercer extends Coercer[Long] {
  override def coerce(input: Any): Long = input match {
    case i: Int => i
    case l: Long => l
    case s: Short => s
    case s: String => s.toLong
  }
}

object DoubleCoercer extends Coercer[Double] {
  override def coerce(input: Any): Double = input match {
    case d: Double => d
    case f: Float => f
    case i: Int => i
    case l: Long => l
    case s: Short => s
    case s: String => s.toDouble
  }
}

object BigDecimalCoercer extends Coercer[BigDecimal] {
  override def coerce(input: Any): BigDecimal = input match {
    case d: Double => BigDecimal(d)
    case f: Float => BigDecimal(f)
    case i: Int => BigDecimal(i)
    case l: Long => BigDecimal(l)
    case s: Short => BigDecimal(s)
    case b: BigInt => BigDecimal(b)
    case b: BigDecimal => b // pass through
    case b: java.math.BigDecimal => b // implicit
    case b: java.math.BigInteger => BigDecimal(b)
    case s: String => BigDecimal(s)
  }
}

object BigIntegerCoercer extends Coercer[BigInt] {
  override def coerce(input: Any): BigInt = input match {
    case i: Int => BigInt(i)
    case l: Long => BigInt(l)
    case s: Short => BigInt(s)
    case b: BigInt => b // pass through
    case b: java.math.BigInteger => b // implicit
    case s: String => BigInt(s)
  }
}