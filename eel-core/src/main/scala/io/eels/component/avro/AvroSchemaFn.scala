package io.eels.component.avro

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, FrameSchema, SchemaType}
import org.apache.avro.{Schema, SchemaBuilder}
import org.codehaus.jackson.node.NullNode

import scala.collection.JavaConverters._

object AvroSchemaFn extends StrictLogging {

  def toAvro(fs: FrameSchema): Schema = {
    val schema = Schema.createRecord("row", null, "io.eels.component.avro", false)
    val fields = fs.columns.map(toAvroField)
    schema.setFields(fields.asJava)
    schema
  }

  def fromAvro(schema: Schema): FrameSchema = {
    val cols = schema.getFields.asScala.map(toColumn)
    FrameSchema(cols.toList)
  }

  def toSchemaType(schema: Schema): SchemaType = {
    schema.getType match {
      case Schema.Type.BOOLEAN => SchemaType.Boolean
      case Schema.Type.DOUBLE => SchemaType.Double
      case Schema.Type.FLOAT => SchemaType.Float
      case Schema.Type.INT => SchemaType.Int
      case Schema.Type.LONG => SchemaType.Long
      case Schema.Type.STRING => SchemaType.String
      case other =>
        logger.warn(s"Unrecognized avro type $other; defaulting to string")
        SchemaType.String
    }
  }

  def toColumn(field: Schema.Field): Column = {
    val schemaType = field.schema.getType match {
      case Schema.Type.UNION =>
         field.schema.getTypes.asScala.filter(_.getType != Schema.Type.NULL).collectFirst {
          case schema => toSchemaType(schema)
        }.getOrElse(sys.error("Union types must define a non null type"))
      case _ => toSchemaType(field.schema)
    }
    Column(field.name, schemaType, true)
  }

  def toAvroField(column: Column): Schema.Field = {
    val schema = column.`type` match {
      case SchemaType.String => SchemaBuilder.builder().stringType()
      case SchemaType.Int => SchemaBuilder.builder().intType()
      case SchemaType.Boolean => Schema.create(Schema.Type.BOOLEAN)
      case SchemaType.Double => Schema.create(Schema.Type.DOUBLE)
      case SchemaType.Float => Schema.create(Schema.Type.FLOAT)
      case SchemaType.Long => Schema.create(Schema.Type.LONG)
      case other =>
        logger.warn(s"Unknown column type ${column.name}; defaulting to string")
        Schema.create(Schema.Type.STRING)
    }

    if (column.nullable) {
      val union = SchemaBuilder.unionOf().nullType().and().`type`(schema).endUnion()
      new Schema.Field(column.name, union, null, NullNode.getInstance())
    } else {
      new Schema.Field(column.name, schema, null, null)
    }
  }
}
