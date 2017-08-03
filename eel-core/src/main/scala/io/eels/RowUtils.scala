package io.eels

import io.eels.coercion.{BigDecimalCoercer, BigIntegerCoercer, BooleanCoercer, ByteCoercer, DateCoercer, DoubleCoercer, FloatCoercer, IntCoercer, LongCoercer, StringCoercer, TimestampCoercer}
import io.eels.schema.{BigIntType, BinaryType, BooleanType, ByteType, CharType, DateType, DecimalType, DoubleType, FloatType, IntType, LongType, StringType, StructType, TimestampMillisType, VarcharType}

object RowUtils {

  /**
    * Accepts a Row and reformats it according to the target schema, using the lookup map
    * for missing or replacement values.
   */
  def rowAlign(row: Row, targetSchema: StructType, lookup: Map[String, Any] = Map.empty): Row = {
    val values = targetSchema.fieldNames().map { name =>
      if (lookup.contains(name)) lookup(name) else row.get(name)
    }
    Row(targetSchema, values.toVector)
  }

  /**
    * Accepts a Row, and returns a new Row where each value in the row has been coerced into
    * the correct type for it's field. For example, if a row has a schema with a String field called foo,
    * and a value for foo that is a Boolean, after calling this method, the boolean would have been
    * coerced into a String.
    */
  def coerce(row: Row): Row = {
    val values = row.schema.fields.zip(row.values).map { case (field, value) =>
      if (value == null) null
      else field.dataType match {
        case StringType | VarcharType(_) | CharType(_)=> StringCoercer.coerce(value)
        case LongType(_) => LongCoercer.coerce(value)
        case IntType(_) => IntCoercer.coerce(value)
        case ByteType(_) => ByteCoercer.coerce(value)
        case BigIntType => BigIntegerCoercer.coerce(value)
        case DoubleType => DoubleCoercer.coerce(value)
        case FloatType => FloatCoercer.coerce(value)
        case BooleanType => BooleanCoercer.coerce(value)
        case BinaryType => value
        case _: DecimalType => BigDecimalCoercer.coerce(value)
        case TimestampMillisType => TimestampCoercer.coerce(value)
        case DateType => DateCoercer.coerce(value)
      }
    }
    Row(row.schema, values)
  }
}