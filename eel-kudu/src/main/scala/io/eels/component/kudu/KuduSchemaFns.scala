package io.eels.component.kudu

import io.eels.schema._
import org.apache.kudu.{ColumnSchema, Schema, Type}

import scala.collection.JavaConverters._

object KuduSchemaFns {

  def fromKuduSchema(schema: Schema): StructType = {
    val fields = schema.getColumns.asScala.map { col =>
      val datatype = col.getType match {
        case Type.BINARY => BinaryType
        case Type.BOOL => BooleanType
        case Type.DOUBLE => DoubleType
        case Type.FLOAT => FloatType
        case Type.INT8 => ByteType.Signed
        case Type.INT16 => ShortType.Signed
        case Type.INT32 => IntType.Signed
        case Type.INT64 => LongType.Signed
        case Type.STRING => StringType
        case Type.UNIXTIME_MICROS => TimestampMicrosType
      }
      Field(col.getName, datatype, nullable = col.isNullable, key = col.isKey)
    }
    StructType(fields)
  }

  def toKuduColumn(field: Field): ColumnSchema = {
    val tpe = field.dataType match {
      case BinaryType => Type.BINARY
      case BooleanType => Type.BOOL
      case DoubleType => Type.DOUBLE
      case FloatType => Type.FLOAT
      case _: ByteType => Type.INT8
      case _: ShortType => Type.INT16
      case _: IntType => Type.INT32
      case _: LongType => Type.INT64
      case StringType => Type.STRING
      case TimestampMicrosType => Type.UNIXTIME_MICROS
    }
    new ColumnSchema.ColumnSchemaBuilder(field.name, tpe).nullable(field.nullable).key(field.key).build()
  }

  // kudu does not support nested structs
  def toKuduSchema(structType: StructType): Schema = {
    val columns = structType.fields.map(toKuduColumn)
    assert(columns.exists(_.isKey == true), "Kudu schema requires at least one column to be marked as a key")
    assert(!columns.filter(_.isKey).exists(_.isNullable), "Kudu does not allow key columns to be nullable")
    new Schema(columns.asJava)
  }
}
