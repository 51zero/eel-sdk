package io.eels

import io.eels.coercion.{BigDecimalCoercer, BigIntegerCoercer, BooleanCoercer, ByteCoercer, DateCoercer, DoubleCoercer, FloatCoercer, IntCoercer, LongCoercer, StringCoercer, TimestampCoercer}
import io.eels.schema.{BigIntType, BinaryType, BooleanType, ByteType, CharType, DateType, DecimalType, DoubleType, FloatType, IntType, LongType, StringType, StructType, TimestampMillisType, VarcharType}

object RowUtils {

  // returns the data in this row as a Map
  def toMap(row: Rec, inputSchema: StructType): Map[String, Any] = {
    row.zip(inputSchema.fieldNames).map { case (value, name) =>
      name -> value
    }.toMap
  }

  // accepts a row with the source schema, and returns a new rec matching the target schema
  // any missing values in the input row are looked up in the map, or an exception is thrown
  def rowAlign(row: Rec, inputSchema: StructType, outputSchema: StructType, lookup: Map[String, Any]): Rec = {
    val map = RowUtils.toMap(row, inputSchema)
    outputSchema.fieldNames.map { name =>
      map.getOrElse(name, lookup(name))
    }.toVector
  }

  def dropIndexes(rec: Rec, indexesToDrop: Array[Int]): Rec = {
    val vector = Vector.newBuilder[Any]
    for (k <- rec.indices) if (!indexesToDrop.contains(k)) vector += rec(k)
    vector.result()
  }

  def replace(rec: Rec, index: Int, fn: (Any) => Any): Rec = rec.updated(index, fn(rec(index)))

  def removeIndex(rec: Rec, index: Int): Rec = rec.slice(0, index) ++ rec.slice(index + 1, rec.length)

  def replace(rec: Rec, index: Int, from: String, target: Any): Rec = {
    val existing = rec(index)
    if (existing == from) {
      rec.updated(index, target)
    } else {
      rec
    }
  }

  def filter(rec: Rec, index: Int, p: (Any) => Boolean): Boolean = p(rec(index))

  def map(rec: Rec, index: Int, fn: Any => Any, caseSensitive: Boolean = true): Rec = {
    val value = rec(index)
    rec.updated(index, value)
  }

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