package io.eels.spark

import io.eels.schema.{BinaryType, BooleanType, ByteType, CharType, DateType, DecimalType, DoubleType, Field, FloatType, IntType, LongType, Precision, Scale, ShortType, StringType, StructType, TimestampMillisType, VarcharType}

object SparkSchemaFns {

  def toSparkSchema(schema: StructType): org.apache.spark.sql.types.StructType = {
    val fields = schema.fields.map { field =>
      val datatype = field.dataType match {
        case StringType => org.apache.spark.sql.types.StringType
        case DecimalType(Precision(precision), Scale(scale)) => org.apache.spark.sql.types.DecimalType(precision, scale)
        case LongType(_) => org.apache.spark.sql.types.LongType
        case IntType(_) => org.apache.spark.sql.types.IntegerType
        case DoubleType => org.apache.spark.sql.types.DoubleType
        case FloatType => org.apache.spark.sql.types.FloatType
        case BooleanType => org.apache.spark.sql.types.BooleanType
        case BinaryType => org.apache.spark.sql.types.BinaryType
        case ByteType(_) => org.apache.spark.sql.types.ByteType
        case TimestampMillisType => org.apache.spark.sql.types.TimestampType
        case ShortType(_) => org.apache.spark.sql.types.ShortType
        case VarcharType(size) => org.apache.spark.sql.types.VarcharType(size)
        case CharType(size) => org.apache.spark.sql.types.CharType(size)
        case DateType => org.apache.spark.sql.types.DateType
      }
      org.apache.spark.sql.types.StructField(field.name, datatype, field.nullable)
    }
    org.apache.spark.sql.types.StructType(fields)
  }

  def fromSparkSchema(schema: org.apache.spark.sql.types.StructType): StructType = {
    val fields = schema.fields.map { field =>
      val datatype = field.dataType match {
        case org.apache.spark.sql.types.StringType => StringType
        case dt: org.apache.spark.sql.types.DecimalType => DecimalType(Precision(dt.precision), Scale(dt.scale))
        case org.apache.spark.sql.types.LongType => LongType.Signed
        case org.apache.spark.sql.types.IntegerType => IntType.Signed
        case org.apache.spark.sql.types.ShortType => ShortType.Signed
        case org.apache.spark.sql.types.DoubleType => DoubleType
        case org.apache.spark.sql.types.FloatType => FloatType
        case org.apache.spark.sql.types.BooleanType => BooleanType
        case org.apache.spark.sql.types.BinaryType => BinaryType
        case org.apache.spark.sql.types.ByteType => ByteType.Signed
        case org.apache.spark.sql.types.TimestampType => TimestampMillisType
        case org.apache.spark.sql.types.VarcharType(size) => VarcharType(size)
        case org.apache.spark.sql.types.CharType(size) => CharType(size)
        case org.apache.spark.sql.types.DateType => DateType
      }
      Field(field.name, datatype, field.nullable)
    }
    StructType(fields)
  }
}
