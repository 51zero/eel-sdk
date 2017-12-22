package io.eels.component.hbase

import java.sql.Timestamp

import io.eels.coercion.Coercer
import org.apache.hadoop.hbase.util.Bytes

/**
  * Helpers for converting (coercing) values to and from EEL types and HBase types
  *
  * Eventually these should move into EEL Core
  */
object HbaseCoercers {

  object StringCoercer extends Coercer[String] {
    override def coerce(input: Any): String = input match {
      case s: String => s
      case _@otherValue => otherValue.toString
    }
  }

  object ByteCoercer extends Coercer[Byte] {
    override def coerce(input: Any): Byte = input match {
      case b: Byte => b
      case c: Char => c.toByte
      case s: Short => s.toByte
      case i: Int => i.toByte
      case l: Long => l.toByte
    }
  }

  object BinaryCoercer extends Coercer[Array[Byte]] {
    override def coerce(input: Any): Array[Byte] = input match {
      case ba: Array[Byte] => ba
      case b: Byte => Bytes.toBytes(b)
      case c: Char => Bytes.toBytes(c)
      case s: Short => Bytes.toBytes(s)
      case i: Int => Bytes.toBytes(i)
      case l: Long => Bytes.toBytes(l)
      case f: Float => Bytes.toBytes(f)
      case d: Double => Bytes.toBytes(d)
      case bd: BigDecimal => Bytes.toBytes(bd.underlying())
      case jbd: java.math.BigDecimal => Bytes.toBytes(jbd)
      case s: String => Bytes.toBytes(s)
    }
  }

  object ShortCoercer extends Coercer[Short] {
    override def coerce(input: Any): Short = input match {
      case s: Short => s
      case b: Byte => b.toShort
      case c: Char => c.toShort
      case i: Int => i.toShort
      case l: Long => l.toShort
      case f: Float => f.toShort
      case d: Double => d.toShort
      case n: java.lang.Number => n.shortValue()
      case s: String => s.toShort
    }
  }

  object IntCoercer extends Coercer[Int] {
    override def coerce(input: Any): Int = input match {
      case i: Int => i
      case b: Byte => b.toInt
      case c: Char => c.toInt
      case s: Short => s.toInt
      case l: Long => l.toInt
      case f: Float => f.toInt
      case d: Double => d.toInt
      case n: java.lang.Number => n.intValue()
      case s: String => s.toInt
    }
  }

  object LongCoercer extends Coercer[Long] {
    override def coerce(input: Any): Long = input match {
      case l: Long => l
      case i: Int => i
      case b: Byte => b.toLong
      case c: Char => c.toLong
      case s: Short => s.toLong
      case f: Float => f.toLong
      case d: Double => d.toLong
      case n: java.lang.Number => n.longValue()
      case s: String => s.toLong
    }
  }

  object FloatCoercer extends Coercer[Float] {
    override def coerce(input: Any): Float = input match {
      case f: Float => f
      case l: Long => l.toFloat
      case i: Int => i.toFloat
      case b: Byte => b.toFloat
      case c: Char => c.toFloat
      case s: Short => s.toFloat
      case d: Double => d.toFloat
      case n: java.lang.Number => n.floatValue()
      case s: String => s.toFloat
    }
  }

  object DoubleCoercer extends Coercer[Double] {
    override def coerce(input: Any): Double = input match {
      case d: Double => d
      case f: Float => f.toDouble
      case l: Long => l.toDouble
      case i: Int => i.toDouble
      case b: Byte => b.toDouble
      case c: Char => c.toDouble
      case s: Short => s.toDouble
      case n: java.lang.Number => n.doubleValue()
      case s: String => s.toDouble
    }
  }

  object DecimalCoercer extends Coercer[java.math.BigDecimal] {
    override def coerce(input: Any): java.math.BigDecimal = input match {
      case jbd: java.math.BigDecimal => jbd
      case bd: BigDecimal => bd.underlying()
      case d: Double => java.math.BigDecimal.valueOf(d)
      case f: Float => java.math.BigDecimal.valueOf(f)
      case l: Long => java.math.BigDecimal.valueOf(l)
      case i: Int => java.math.BigDecimal.valueOf(i)
      case b: Byte => java.math.BigDecimal.valueOf(b)
      case c: Char => java.math.BigDecimal.valueOf(c)
      case s: Short => java.math.BigDecimal.valueOf(s)
      case n: java.lang.Number => java.math.BigDecimal.valueOf(n.doubleValue())
      case s: String => new java.math.BigDecimal(s)
    }
  }

  object TimestampCoercer extends Coercer[Timestamp] {
    override def coerce(input: Any): Timestamp = input match {
      case t: Timestamp => t
      case l: Long => new Timestamp(l)
      case s: String => Timestamp.valueOf(s)
    }
  }

  object BooleanCoercer extends Coercer[Boolean] {
    override def coerce(input: Any): Boolean = input match {
      case bl: Boolean => bl
      case jbd: java.math.BigDecimal => numberToBoolean(jbd.longValue())
      case bd: BigDecimal => numberToBoolean(bd.longValue())
      case d: Double => numberToBoolean(d.toLong)
      case f: Float => numberToBoolean(f.toLong)
      case l: Long => numberToBoolean(l.toLong)
      case i: Int => numberToBoolean(i.toLong)
      case b: Byte => numberToBoolean(b.toLong)
      case c: Char => numberToBoolean(c.toLong)
      case s: Short => numberToBoolean(s.toLong)
      case n: java.lang.Number => numberToBoolean(n.longValue())
      case s: String => s.toBoolean
    }
  }

  private def numberToBoolean(value: Long): Boolean = value == 1
}
