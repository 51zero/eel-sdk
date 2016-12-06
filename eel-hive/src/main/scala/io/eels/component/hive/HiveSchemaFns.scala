package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.schema._
import org.apache.hadoop.hive.metastore.api.FieldSchema

// createReader FrameSchema from hive FieldSchemas
// see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
object HiveSchemaFns extends Logging {

  val VarcharRegex = "varchar\\((.*?)\\)".r
  val DecimalRegex = "decimal\\((\\d+),(\\d+)\\)".r
  val StructRegex = "struct<(.*?)>".r

  // converts an eel field into a hive FieldSchema
  def toHiveField(field: Field): FieldSchema = new FieldSchema(field.name.toLowerCase(), toHiveType(field), field.comment.orNull)

  // converts an eel Schema into a list of hive FieldSchema's
  def toHiveFields(schema: StructType): List[FieldSchema] = toHiveFields(schema.fields)

  // converts a list of eel fields into a list of hive FieldSchema's
  def toHiveFields(fields: List[Field]): List[FieldSchema] = fields.map(toHiveField)

  /**
    * Converts a hive FieldSchema into an eel Column type, with the given nullability.
    * Nullability has to be specified manually, since all hive fields are always nullable, but eel supports non nulls too
    */
  def fromHiveField(fieldSchema: FieldSchema, nullable: Boolean): Field =
    fromHive(fieldSchema.getName, fieldSchema.getType, nullable, fieldSchema.getComment)

  def fromHive(name: String, datatype: String, nullable: Boolean, comment: String): Field = {
    val field = datatype match {
      case "bigint" => Field(name, BigIntType, nullable)
      case "binary" => Field(name, BinaryType, nullable)
      case "boolean" => Field(name, BooleanType, nullable)
      case "double" => Field(name, DoubleType, nullable)
      case "float" => Field(name, FloatType, nullable)
      case "int" => Field(name, IntType.Signed, nullable)
      case "smallint" => Field(name, ShortType, nullable)
      case "tinyint" => Field(name, ShortType, nullable)
      case "char" => Field(name, StringType, nullable)
      case "string" => Field(name, StringType, nullable)
      case "date" => Field(name, DateType, nullable)
      case "timestamp" => Field(name, TimestampType, nullable)
      case DecimalRegex(scale, precision) =>
        Field(name, DecimalType(Scale(scale.toInt), Precision(precision.toInt)), nullable)
      case VarcharRegex(precision) =>
        Field(name, VarcharType(precision.toInt), nullable)
      case StructRegex(structType) =>
        val fields = structType.split(",").map { it =>
          val parts = it.split(":")
          fromHive(parts(0), parts(1), nullable, null)
        }
        Field.createStructField(name, fields).withNullable(nullable)
      case _ =>
        logger.warn(s"Unknown hive type $datatype; defaulting to string")
        Field(name, StringType, nullable)
    }
    field.withComment(comment)
  }

  /**
    * Returns the hive type for the given field
    */
  def toHiveType(field: Field): String = field.dataType match {
    case BigIntType => "bigint"
    case BooleanType => "boolean"
    case DateType => "date"
    case DecimalType(precision, scale) => s"decimal(${scale.value},${precision.value})"
    case DoubleType => "double"
    case FloatType => "float"
    case i: IntType => "int"
    case l: LongType => "bigint"
    case ShortType => "smallint"
    case StringType => "string"
    case TimestampType => "timestamp"
    case StructType(fields) => toStructDDL(fields)
    case _ =>
      logger.warn(s"No conversion from eel type [${field.dataType}] to hive type; defaulting to string")
      "string"
  }

  def toStructDDL(fields: List[Field]): String = {
    val types = fields.map { it => it.name + ":" + toHiveType(it) }.mkString(",")
    s"struct<$types>"
  }
}
