package io.eels.component.hive

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Precision
import io.eels.schema.Scale
import io.eels.schema.Schema
import io.eels.util.Logging
import org.apache.hadoop.hive.metastore.api.FieldSchema

// create FrameSchema from hive FieldSchemas
// see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
object HiveSchemaFns : Logging {

  val VarcharRegex = "varchar\\((.*?)\\)".toRegex()
  val DecimalRegex = "decimal\\((\\d+),(\\d+)\\)".toRegex()

  // converts an eel column into a hive FieldSchema
  fun toHiveField(field: Field): FieldSchema = FieldSchema(field.name.toLowerCase(), toHiveType(field), field.comment)

  // converts an eel column into a list of hive FieldSchema's
  fun toHiveFields(schema: Schema): List<FieldSchema> = toHiveFields(schema.fields)

  // converts a list of eel columns into a list of hive FieldSchema's
  fun toHiveFields(fields: List<Field>): List<FieldSchema> = fields.map { toHiveField(it) }

  /**
   * converts a hive FieldSchema into an eel Column type, with the given nullability.
   * Nullability has to be specified manually, since all hive fields are always nullable, but eel supports non nulls too
   */
  fun fromHiveField(fieldSchema: FieldSchema, nullable: Boolean): Field {
    val (ColumnType, precision, scale) = toFieldType(fieldSchema.type)
    return Field(fieldSchema.name, ColumnType, nullable, precision = precision, scale = scale, comment = fieldSchema.comment)
  }

  // returns the eel columnType, precision and scale for a given hive field type "text" description
  fun toFieldType(str: String): Triple<FieldType, Precision, Scale> = when (str) {
    "tinyint" -> Triple(FieldType.Short, Precision(0), Scale(0))
    "smallint" -> Triple(FieldType.Short, Precision(0), Scale(0))
    "int" -> Triple(FieldType.Int, Precision(0), Scale(0))
    "boolean" -> Triple(FieldType.Boolean, Precision(0), Scale(0))
    "bigint" -> Triple(FieldType.BigInt, Precision(0), Scale(0))
    "float" -> Triple(FieldType.Float, Precision(0), Scale(0))
    "double" -> Triple(FieldType.Double, Precision(0), Scale(0))
    "string" -> Triple(FieldType.String, Precision(0), Scale(0))
    "binary" -> Triple(FieldType.Binary, Precision(0), Scale(0))
    "char" -> Triple(FieldType.String, Precision(0), Scale(0))
    "date" -> Triple(FieldType.Date, Precision(0), Scale(0))
    "timestamp" -> Triple(FieldType.Timestamp, Precision(0), Scale(0))
  //DecimalRegex(precision, scale) -> (ColumnType.Decimal, precision.toInt, scale.toInt)
  //VarcharRegex(precision) -> (ColumnType.String, precision.toInt, 0)
    else -> {
      logger.warn("Unknown hive type $str; defaulting to string")
      Triple(FieldType.String, Precision(0), Scale(0))
    }
  }

  /**
   * Returns the hive type for the given field
   */
  fun toHiveType(field: Field): String = when (field.type) {
    FieldType.BigInt -> "bigint"
    FieldType.Boolean -> "boolean"
    FieldType.Decimal -> "decimal(${field.scale},${field.precision})"
    FieldType.Double -> "double"
    FieldType.Float -> "float"
    FieldType.Int -> "int"
    FieldType.Long -> "bigint"
    FieldType.Short -> "smallint"
    FieldType.String -> "string"
    FieldType.Timestamp -> "timestamp"
    FieldType.Date -> "date"
    FieldType.Struct -> toStructDDL(field)
    else -> {
      logger.warn("No conversion from field type ${field.`type`} to hive type; defaulting to string")
      "string"
    }
  }

  fun toStructDDL(field: Field): String {
    require(field.type == FieldType.Struct, { "Invoked struct method on non struct type" })
    val types = field.fields.map { it.name + ":" + toHiveType(it) }.joinToString(",")
    return "struct<$types>"
  }
}
