package io.eels.component.avro

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{SchemaType, Column, FrameSchema}
import org.apache.avro.Schema
import scala.collection.JavaConverters._

object AvroSchemaGen extends StrictLogging {

  def apply(fs: FrameSchema): Schema = {
    val schema = Schema.createRecord("row", "", "io.eels.avro.generated", false)
    val fields = fs.columns.map(toSchemaField)
    schema.setFields(fields.asJava)
    schema
  }

  def toSchemaField(column: Column): Schema.Field = column.`type` match {
    case SchemaType.String => new Schema.Field(column.name, Schema.create(Schema.Type.STRING), "", null)
    case SchemaType.Int => new Schema.Field(column.name, Schema.create(Schema.Type.INT), "", null)
    case SchemaType.Boolean => new Schema.Field(column.name, Schema.create(Schema.Type.BOOLEAN), "", null)
    case SchemaType.Double => new Schema.Field(column.name, Schema.create(Schema.Type.DOUBLE), "", null)
    case SchemaType.Float => new Schema.Field(column.name, Schema.create(Schema.Type.FLOAT), "", null)
    case SchemaType.Long => new Schema.Field(column.name, Schema.create(Schema.Type.LONG), "", null)
    case other =>
      logger.warn("Unknown column type; defaulting to string")
      new Schema.Field(column.name, Schema.create(Schema.Type.STRING), "", null)
  }
}
