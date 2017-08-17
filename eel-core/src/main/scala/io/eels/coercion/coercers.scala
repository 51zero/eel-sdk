package io.eels.coercion

import java.sql.Timestamp
import java.util.Date

import io.eels.Rec

import scala.collection.JavaConverters._

/**
  * A Coercer will coerce input values into an output value of type T.
  * The types of coercion supported depends on the implementation but for example,
  * a Double coercer might support converting a String, Float and Long to a Double.
  *
  * Note: No-coercer should be lossy.
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
    case bigint: BigInt => bigint.longValue == 1
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
    case bigint: BigInt if bigint.isValidLong => new Timestamp(bigint.longValue)
    case bigint: java.math.BigInteger => new Timestamp(bigint.longValueExact)
    case bigdec: java.math.BigDecimal => new Timestamp(bigdec.longValueExact)
  }
}

object DateCoercer extends Coercer[java.util.Date] {
  override def coerce(input: Any): Date = input match {
    case date: Date => date
    case timestamp: Long => new Date(timestamp)
    case str: String => new Date(str.toLong)
    case timestamp:Timestamp => new Date(timestamp.getTime)
  }
}

object IntCoercer extends Coercer[Int] {
  override def coerce(input: Any): Int = input match {
    case i: Int => i // passthrough
    case l: Long if Int.MinValue <= l && l <= Int.MaxValue => l.toInt
    case b: Byte => b
    case s: Short => s
    case s: String => s.toInt
    case bigint: BigInt if bigint.isValidInt => bigint.intValue
    case bigdec: BigDecimal if bigdec.isValidInt => bigdec.intValue
    case bigint: java.math.BigInteger => bigint.intValueExact
    case bigdec: java.math.BigDecimal => bigdec.intValueExact
  }
}

object ShortCoercer extends Coercer[Short] {
  override def coerce(input: Any): Short = input match {
    case b: Byte => b
    case s: Short => s
    case i: Int if Short.MinValue <= i && i <= Short.MaxValue => i.toShort
    case l: Long if Short.MinValue <= l && l <= Short.MaxValue => l.toShort
    case s: String => s.toShort
    case bigint: BigInt if bigint.isValidShort => bigint.shortValue
    case bigdec: BigDecimal if bigdec.isValidShort => bigdec.shortValue
    case bigint: java.math.BigInteger => bigint.shortValueExact
    case bigdec: java.math.BigDecimal => bigdec.shortValueExact
  }
}

object ByteCoercer extends Coercer[Byte] {
  override def coerce(input: Any): Byte = input match {
    case b: Byte => b
    case s: String => s.toByte
    case s: Short if Byte.MinValue <= s && s <= Byte.MaxValue => s.toByte
    case i: Int if Byte.MinValue <= i && i <= Byte.MaxValue => i.toByte
    case l: Long if Byte.MinValue <= l && l <= Byte.MaxValue => l.toByte
    case bigint: BigInt if bigint.isValidByte => bigint.byteValue
    case bigdec: BigDecimal if bigdec.isValidByte => bigdec.byteValue
    case bigint: java.math.BigInteger => bigint.byteValueExact()
    case bigdec: java.math.BigDecimal => bigdec.byteValueExact()
  }
}

object LongCoercer extends Coercer[Long] {
  override def coerce(input: Any): Long = input match {
    case l: Long => l // passthrough
    case b: Byte => b
    case i: Int => i
    case s: Short => s
    case s: String => s.toLong
    case t: Timestamp => t.getTime
    case bigint: BigInt if bigint.isValidLong => bigint.longValue
    case bigdec: BigDecimal if bigdec.isValidLong => bigdec.longValue
    case bigint: java.math.BigInteger => bigint.longValueExact()
    case bigdec: java.math.BigDecimal => bigdec.longValueExact()
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

object StringCoercer extends Coercer[String] {
  override def coerce(input: Any): String = input match {
    case s: String => s
    case other => other.toString
  }
}

object BigIntegerCoercer extends Coercer[BigInt] {
  override def coerce(input: Any): BigInt = input match {
    case b: BigInt => b // pass through
    case bigdec: BigDecimal if bigdec.isWhole => bigdec.toBigInt()
    case bigint: java.math.BigInteger => bigint
    case bigdec: java.math.BigDecimal => bigdec.toBigInteger
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
    case rec: Rec => rec
    case product: Product => product.productIterator.toSeq
    case col: java.util.Iterator[Any] => col.asScala.toSeq
    case col: java.util.Collection[Any] => col.asScala.toSeq
  }
}