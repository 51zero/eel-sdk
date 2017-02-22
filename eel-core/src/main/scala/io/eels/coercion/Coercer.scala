package io.eels.coercion

import java.sql.Timestamp
import scala.collection.JavaConverters._

/**
  * A Coercer will coerce input values into an output value of type T.
  * The types of coercion supported depends on the implementation but for example,
  * a Double coercer might support converting a String, Float and Long to a Double.
  */
trait Coercer[T] {
  def coerce(input: Any): T
}

object BooleanCoercer extends Coercer[Boolean] {
  override def coerce(input: Any): Boolean = input match {
    case b: Boolean => b // passthrough
    case l: Long => l == 1
    case i: Int => i == 1
    case s: Short => s == 1
    case b: Byte => b == 1
    case s: String => s == "true"
  }
}

object MapCoercer extends Coercer[Map[Any, Any]] {
  override def coerce(input: Any): Map[Any, Any] = input match {
    case map: scala.collection.immutable.Map[_, _] => map.toMap
    case map: scala.collection.mutable.Map[_, _] => map.toMap
    case map: java.util.Map[_, _] => map.asScala.toMap
  }
}

object TimestampCoercer extends Coercer[java.sql.Timestamp] {
  override def coerce(input: Any): Timestamp = input match {
    case t: Timestamp => t // passthrough
    case l: Long => new Timestamp(l)
  }
}

object IntCoercer extends Coercer[Int] {
  override def coerce(input: Any): Int = input match {
    case i: Int => i // passthrough
    case b: Byte => b
    case s: Short => s
    case s: String => s.toInt
  }
}

object LongCoercer extends Coercer[Long] {
  override def coerce(input: Any): Long = input match {
    case l: Long => l // passthrough
    case b: Byte => b
    case i: Int => i
    case s: Short => s
    case s: String => s.toLong
  }
}

object FloatCoercer extends Coercer[Float] {
  override def coerce(input: Any): Float = input match {
    case f: Float => f // passthrough
    case i: Int => i
    case b: Byte => b
    case l: Long => l
    case s: Short => s
    case s: String => s.toFloat
  }
}

object DoubleCoercer extends Coercer[Double] {
  override def coerce(input: Any): Double = input match {
    case d: Double => d // passthrough
    case b: Byte => b
    case f: Float => f
    case i: Int => i
    case l: Long => l
    case s: Short => s
    case s: String => s.toDouble
  }
}

object BigDecimalCoercer extends Coercer[BigDecimal] {
  override def coerce(input: Any): BigDecimal = input match {
    case b: BigDecimal => b // pass through
    case d: Double => BigDecimal.decimal(d)
    case f: Float => BigDecimal(f)
    case i: Int => BigDecimal(i)
    case l: Long => BigDecimal(l)
    case s: Short => BigDecimal(s)
    case b: BigInt => BigDecimal(b)
    case b: java.math.BigDecimal => b // implicit
    case b: java.math.BigInteger => BigDecimal(b)
    case s: String => BigDecimal(s)
  }
}

object BigIntegerCoercer extends Coercer[BigInt] {
  override def coerce(input: Any): BigInt = input match {
    case b: BigInt => b // pass through
    case i: Int => BigInt(i)
    case l: Long => BigInt(l)
    case s: Short => BigInt(s)
    case b: java.math.BigInteger => b // implicit
    case s: String => BigInt(s)
  }
}

object SequenceCoercer extends Coercer[Seq[Any]] {

  override def coerce(input: Any): Seq[Any] = input match {
    case iter: Iterable[Any] => iter.toSeq
    case array: Array[_] => array
    case seq: Seq[_] => seq
    case seq: Seq[Any] => seq
    case col: java.util.Collection[Any] => col.asScala.toSeq
  }
}