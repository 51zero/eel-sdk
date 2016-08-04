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
  val StructRegex = "struct<(.*?)>".toRegex()

  // converts an eel field into a hive FieldSchema
  fun toHiveField(field: Field): FieldSchema = FieldSchema(field.name.toLowerCase(), toHiveType(field), field.comment)

  // converts an eel Schema into a list of hive FieldSchema's
  fun toHiveFields(schema: Schema): List<FieldSchema> = toHiveFields(schema.fields)

  // converts a list of eel fields into a list of hive FieldSchema's
  fun toHiveFields(fields: List<Field>): List<FieldSchema> = fields.map { toHiveField(it) }

  /**
   * Converts a hive FieldSchema into an eel Column type, with the given nullability.
   * Nullability has to be specified manually, since all hive fields are always nullable, but eel supports non nulls too
   */
  fun fromHiveField(fieldSchema: FieldSchema, nullable: Boolean): Field =
      fromHive(fieldSchema.name, fieldSchema.type, nullable, fieldSchema.comment)

  fun fromHive(name: String, type: String, nullable: Boolean, comment: String?): Field {
    return when {
      type == "tinyint" -> Field(name, FieldType.Short, nullable, Precision(0), Scale(0))
      type == "smallint" -> Field(name, FieldType.Short, nullable, Precision(0), Scale(0))
      type == "int" -> Field(name, FieldType.Int, nullable, Precision(0), Scale(0))
      type == "boolean" -> Field(name, FieldType.Boolean, nullable, Precision(0), Scale(0))
      type == "bigint" -> Field(name, FieldType.BigInt, nullable, Precision(0), Scale(0))
      type == "float" -> Field(name, FieldType.Float, nullable, Precision(0), Scale(0))
      type == "double" -> Field(name, FieldType.Double, nullable, Precision(0), Scale(0))
      type == "string" -> Field(name, FieldType.String, nullable, Precision(0), Scale(0))
      type == "binary" -> Field(name, FieldType.Binary, nullable, Precision(0), Scale(0))
      type == "char" -> Field(name, FieldType.String, nullable, Precision(0), Scale(0))
      type == "date" -> Field(name, FieldType.Date, nullable, Precision(0), Scale(0))
      type == "timestamp" -> Field(name, FieldType.Timestamp, nullable, Precision(0), Scale(0))
      DecimalRegex.matches(type) -> {
        val match = DecimalRegex.matchEntire(type)!!
        Field(name, FieldType.Decimal, nullable, Precision(match.groupValues[2].toInt()), Scale(match.groupValues[1].toInt()))
      }
      VarcharRegex.matches(type) -> {
        val match = VarcharRegex.matchEntire(type)!!
        Field(name, FieldType.String, nullable, Precision(match.groupValues[1].toInt()))
      }
      StructRegex.matches(type) -> {
        val value = StructRegex.matchEntire(type)!!.groupValues[1]
        val fields = value.split(",").map {
          val (name, type) = it.split(":")
          fromHive(name, type, nullable, null)
        }
        Field.createStruct(name, fields).withNullable(nullable)
      }
      else -> {
        logger.warn("Unknown hive type $type; defaulting to string")
        Field(name, FieldType.String, nullable, Precision(0), Scale(0))
      }
    }.withComment(comment)
  }

  /**
   * Returns the hive type for the given field
   */
  fun toHiveType(field: Field): String = when (field.type) {
    FieldType.BigInt -> "bigint"
    FieldType.Boolean -> "boolean"
    FieldType.Decimal -> "decimal(${field.scale.value},${field.precision.value})"
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
