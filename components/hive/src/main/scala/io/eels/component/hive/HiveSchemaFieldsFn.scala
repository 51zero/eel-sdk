package io.eels.component.hive

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, SchemaType}
import org.apache.hadoop.hive.metastore.api.FieldSchema

object HiveSchemaFieldsFn extends StrictLogging {

  def apply(columns: List[Column]): List[FieldSchema] = columns.map(fieldSchema)

  def fieldSchema(column: Column): FieldSchema = {
    new FieldSchema(column.name, hiveType(column), "Created by eel-sdk")
  }

  def hiveType(column: Column): String = column.`type` match {
    case SchemaType.String => "string"
    case _ =>
      logger.warn(s"No conversion from schema type ${column.`type`} to hive type; defaulting to string")
      "string"
  }
}
