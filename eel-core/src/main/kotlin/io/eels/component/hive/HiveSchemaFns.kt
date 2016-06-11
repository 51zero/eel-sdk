package io.eels.component.hive

import io.eels.schema.Column
import io.eels.schema.ColumnType
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
  fun toHiveField(column: Column): FieldSchema = FieldSchema(column.name.toLowerCase(), toHiveType(column), null)

  // converts an eel column into a list of hive FieldSchema's
  fun toHiveFields(schema: Schema): List<FieldSchema> = toHiveFields(schema.columns)

  // converts a list of eel columns into a list of hive FieldSchema's
  fun toHiveFields(columns: List<Column>): List<FieldSchema> = columns.map { toHiveField(it) }

  /**
   * converts a hive FieldSchema into an eel Column type, with the given nullability.
   * Nullability has to be specified manually, since all hive fields are always nullable, but eel supports non nulls too
   */
  fun fromHiveField(fieldSchema: FieldSchema, nullable: Boolean): Column {
    val (ColumnType, precision, scale) = toColumnType(fieldSchema.type)
    return Column(fieldSchema.name, ColumnType, nullable, precision = precision, scale = scale, comment = fieldSchema.comment)
  }

  // returns the eel columnType, precision and scale for a given hive field type "text" description
  fun toColumnType(str: String): Triple<ColumnType, Precision, Scale> = when (str) {
    "tinyint" -> Triple(ColumnType.Short, Precision(0), Scale(0))
    "smallint" -> Triple(ColumnType.Short, Precision(0), Scale(0))
    "int" -> Triple(ColumnType.Int, Precision(0), Scale(0))
    "boolean" -> Triple(ColumnType.Boolean, Precision(0), Scale(0))
    "bigint" -> Triple(ColumnType.BigInt, Precision(0), Scale(0))
    "float" -> Triple(ColumnType.Float, Precision(0), Scale(0))
    "double" -> Triple(ColumnType.Double, Precision(0), Scale(0))
    "string" -> Triple(ColumnType.String, Precision(0), Scale(0))
    "binary" -> Triple(ColumnType.Binary, Precision(0), Scale(0))
    "char" -> Triple(ColumnType.String, Precision(0), Scale(0))
    "date" -> Triple(ColumnType.Date, Precision(0), Scale(0))
    "timestamp" -> Triple(ColumnType.Timestamp, Precision(0), Scale(0))
  //DecimalRegex(precision, scale) -> (ColumnType.Decimal, precision.toInt, scale.toInt)
  //VarcharRegex(precision) -> (ColumnType.String, precision.toInt, 0)
    else -> {
      logger.warn("Unknown schema type $str; defaulting to string")
      Triple(ColumnType.String, Precision(0), Scale(0))
    }
  }

  /**
   * Returns the hive column type for the given column
   */
  fun toHiveType(column: Column): String = when (column.type) {
    ColumnType.BigInt -> "bigint"
    ColumnType.Boolean -> "boolean"
    ColumnType.Decimal -> "decimal(${column.scale},${column.precision})"
    ColumnType.Double -> "double"
    ColumnType.Float -> "float"
    ColumnType.Int -> "int"
    ColumnType.Long -> "bigint"
    ColumnType.Short -> "smallint"
    ColumnType.String -> "string"
    ColumnType.Timestamp -> "timestamp"
    ColumnType.Date -> "date"
    else -> {
      logger.warn("No conversion from column type ${column.`type`} to hive type; defaulting to string")
      "string"
    }
  }
}
