package io.eels.component.hive

import com.sksamuel.scalax.NonEmptyString
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, FrameSchema, SchemaType}
import org.apache.hadoop.hive.metastore.api.FieldSchema

// create FrameSchema from hive FieldSchemas
// see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
object FrameSchemaFn extends StrictLogging {

  def apply(schemas: Seq[FieldSchema]): FrameSchema = {

    val columns = schemas.map { s =>
      val (schemaType, precision, scale) = toSchemaType(s.getType)
      Column(s.getName, schemaType, false, precision = precision, scale = scale, comment = NonEmptyString(s.getComment))
    }

    FrameSchema(columns.toList)
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
}
