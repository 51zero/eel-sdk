package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Precision
import io.eels.schema.Scale
import io.eels.schema.Schema
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
  def toHiveFields(schema: Schema): List[FieldSchema] = toHiveFields(schema.fields)

  // converts a list of eel fields into a list of hive FieldSchema's
  def toHiveFields(fields: List[Field]): List[FieldSchema] = fields.map(toHiveField)

  /**
    * Converts a hive FieldSchema into an eel Column type, with the given nullability.
    * Nullability has to be specified manually, since all hive fields are always nullable, but eel supports non nulls too
    */
  def fromHiveField(fieldSchema: FieldSchema, nullable: Boolean): Field =
    fromHive(fieldSchema.getName, fieldSchema.getType, nullable, fieldSchema.getComment)

  def fromHive(name: String, `type`: String, nullable: Boolean, comment: String): Field = {
    val field = `type` match {
      case "tinyint" => Field(name, FieldType.Short, nullable, Precision(0), Scale(0))
      case "smallint" => Field(name, FieldType.Short, nullable, Precision(0), Scale(0))
      case "int" => Field(name, FieldType.Int, nullable, Precision(0), Scale(0))
      case "boolean" => Field(name, FieldType.Boolean, nullable, Precision(0), Scale(0))
      case "bigint" => Field(name, FieldType.BigInt, nullable, Precision(0), Scale(0))
      case "float" => Field(name, FieldType.Float, nullable, Precision(0), Scale(0))
      case "double" => Field(name, FieldType.Double, nullable, Precision(0), Scale(0))
      case "string" => Field(name, FieldType.String, nullable, Precision(0), Scale(0))
      case "binary" => Field(name, FieldType.Binary, nullable, Precision(0), Scale(0))
      case "char" => Field(name, FieldType.String, nullable, Precision(0), Scale(0))
      case "date" => Field(name, FieldType.Date, nullable, Precision(0), Scale(0))
      case "timestamp" => Field(name, FieldType.Timestamp, nullable, Precision(0), Scale(0))
      case DecimalRegex(scale, precision) =>
        Field(name, FieldType.Decimal, nullable, Precision(precision.toInt), Scale(scale.toInt))
      case VarcharRegex(precision) =>
        Field(name, FieldType.String, nullable, Precision(precision.toInt))
      case StructRegex(structType) =>
        val fields = structType.split(",").map { it =>
          val parts = it.split(":")
          fromHive(parts(0), parts(1), nullable, null)
        }
        Field.createStruct(name, fields).withNullable(nullable)
      case _ =>
        logger.warn("Unknown hive type $type; defaulting to string")
        Field(name, FieldType.String, nullable, Precision(0), Scale(0))
    }
    field.withComment(comment)
  }

  /**
    * Returns the hive type for the given field
    */
  def toHiveType(field: Field): String = field.`type` match {
    case FieldType.BigInt => "bigint"
    case FieldType.Boolean => "boolean"
    case FieldType.Decimal => s"decimal(${field.scale.value},${field.precision.value})"
    case FieldType.Double => "double"
    case FieldType.Float => "float"
    case FieldType.Int => "int"
    case FieldType.Long => "bigint"
    case FieldType.Short => "smallint"
    case FieldType.String => "string"
    case FieldType.Timestamp => "timestamp"
    case FieldType.Date => "date"
    case FieldType.Struct => toStructDDL(field)
    case _ =>
      logger.warn(s"No conversion from field type ${field.`type`} to hive type; defaulting to string")
      "string"
  }

  def toStructDDL(field: Field): String = {
    require(field.`type` == FieldType.Struct, "Invoked struct method on non struct type")
    val types = field.fields.map { it => it.name + ":" + toHiveType(it) }.mkString(",")
    s"struct<$types>"
  }
}
