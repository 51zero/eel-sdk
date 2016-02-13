package io.eels.component.hive

import com.sksamuel.scalax.NonEmptyString
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, FrameSchema, SchemaType}
import org.apache.hadoop.hive.metastore.api.FieldSchema

import scala.collection.mutable

// create FrameSchema from hive FieldSchemas
object FrameSchemaFn extends StrictLogging {

  def apply(schema: mutable.Buffer[FieldSchema]): FrameSchema = {

    val columns = schema.map { s =>
      val (schemaType, precision) = toSchemaType(s.getType)
      Column(s.getName, schemaType, false, precision = precision, comment = NonEmptyString(s.getComment))
    }

    FrameSchema(columns.toList)
  }

  val VarcharRegex = "varchar\\(\\d+\\)".r

  def toSchemaType(str: String): (SchemaType, Int) = str match {
    case "int" => (SchemaType.Int, 0)
    case "bigint" => (SchemaType.BigInt, 0)
    case "double" => (SchemaType.Double, 0)
    case "string" => (SchemaType.String, 0)
    case VarcharRegex(precision) => (SchemaType.String, precision.toInt)
    case other =>
      logger.warn(s"Unknown schema type $other; defaulting to varchar")
      (SchemaType.String, 255)
  }
}
