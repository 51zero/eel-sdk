package io.eels.component.hive

import com.sksamuel.scalax.NonEmptyString
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, Schema, SchemaType}
import org.apache.hadoop.hive.metastore.api.FieldSchema

// create FrameSchema from hive FieldSchemas
// see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
object HiveSchemaFns extends StrictLogging {

  def toHiveFields(schema: Schema): Seq[FieldSchema] = toHiveFields(schema.columns)
  def toHiveFields(columns: Seq[Column]): Seq[FieldSchema] = columns.map { column =>
    new FieldSchema(column.name, toHiveType(column), null)
  }

  def fromHiveFields(schemas: Seq[FieldSchema]): Schema = {
    logger.debug("Building frame schame from hive field schemas=" + schemas)
    val columns = schemas.map { s =>
      val (schemaType, precision, scale) = toSchemaType(s.getType)
      Column(s.getName, schemaType, true, precision = precision, scale = scale, comment = NonEmptyString(s.getComment))
    }
    Schema(columns.toList)
  }

  val VarcharRegex = "varchar\\((\\d+\\))".r
  val DecimalRegex = "decimal\\((\\d+),(\\d+\\))".r

  def toSchemaType(str: String): (SchemaType, Int, Int) = str match {
    case "tinyint" => (SchemaType.Short, 0, 0)
    case "smallint" => (SchemaType.Short, 0, 0)
    case "int" => (SchemaType.Int, 0, 0)
    case "bigint" => (SchemaType.BigInt, 0, 0)
    case "float" => (SchemaType.Float, 0, 0)
    case "double" => (SchemaType.Double, 0, 0)
    case "string" => (SchemaType.String, 0, 0)
    case "char" => (SchemaType.String, 0, 0)
    case DecimalRegex(precision, scale) => (SchemaType.Decimal, precision.toInt, scale.toInt)
    case VarcharRegex(precision) => (SchemaType.String, precision.toInt, 0)
    case other =>
      logger.warn(s"Unknown schema type $other; defaulting to string")
      (SchemaType.String, 0, 0)
  }


  def toHiveType(column: Column): String = column.`type` match {
    case SchemaType.BigInt => "bigint"
    case SchemaType.Double => "double"
    case SchemaType.Float => "float"
    case SchemaType.Int => "int"
    case SchemaType.Long => "bigint"
    case SchemaType.String => "string"
    case SchemaType.Short => "smallint"
    case _ =>
      logger.warn(s"No conversion from schema type ${column.`type`} to hive type; defaulting to string")
      "string"
  }
}
