package io.eels

import com.sksamuel.scalax.Logging

trait Converter[T] {
  def apply(value: Any): T
}

object Converter extends Logging {

  def apply(schemaType: SchemaType): Converter[_] = schemaType match {
    case SchemaType.Boolean => BooleanConverter
    case SchemaType.Float => FloatConverter
    case SchemaType.Double => DoubleConverter
    case SchemaType.Int => IntConverter
    case SchemaType.Long => LongConverter
    case SchemaType.Short => ShortConverter
    case SchemaType.String => StringConverter
    case other =>
      logger.warn(s"No converter exists for schemaType=$schemaType; defaulting to StringConverter")
      StringConverter
  }

  class OptionalConverter(nested: Converter[_]) extends Converter[Any] {
    override def apply(value: Any): Any = Option(value).map(nested.apply).orNull
  }

  object StringConverter extends Converter[String] {
    override def apply(value: Any): String = value.toString
  }

  object BooleanConverter extends Converter[Boolean] {
    override def apply(value: Any): Boolean = value match {
      case b: Boolean => b
      case "true" => true
      case "false" => false
      case other => sys.error(s"Cannot convert $other to boolean")
    }
  }

  object IntConverter extends Converter[Int] {
    override def apply(value: Any): Int = value match {
      case int: Int => int
      case long: Long => long.toInt
      case _ => value.toString.toInt
    }
  }

  object ShortConverter extends Converter[Short] {
    override def apply(value: Any): Short = value match {
      case s: Short => s
      case int: Int => int.toShort
      case long: Long => long.toShort
      case _ => value.toString.toShort
    }
  }

  object LongConverter extends Converter[Long] {
    override def apply(value: Any): Long = value match {
      case int: Int => int.toLong
      case long: Long => long
      case _ => value.toString.toLong
    }
  }

  object DoubleConverter extends Converter[Double] {
    override def apply(value: Any): Double = value match {
      case float: Float => float.toDouble
      case double: Double => double
      case int: Int => int.toDouble
      case long: Long => long.toDouble
      case _ => value.toString.toDouble
    }
  }

  object FloatConverter extends Converter[Float] {
    override def apply(value: Any): Float = value match {
      case float: Float => float
      case double: Double => double.toFloat
      case int: Int => int.toFloat
      case long: Long => long.toFloat
      case _ => value.toString.toFloat
    }
  }
}
