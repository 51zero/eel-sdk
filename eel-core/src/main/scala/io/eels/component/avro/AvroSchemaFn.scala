package io.eels.component.avro

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, Schema, SchemaType}
import org.apache.avro.{Schema => AvroSchema, SchemaBuilder}
import org.codehaus.jackson.node.NullNode

import scala.collection.JavaConverters._

object AvroSchemaFn extends StrictLogging {

  def toAvro(fs: Schema): AvroSchema = {
    val avro = AvroSchema.createRecord("row", null, "io.eels.component.avro", false)
    val fields = fs.columns.map(toAvroField)
    avro.setFields(fields.asJava)
    avro
  }

  def fromAvro(avro: AvroSchema): Schema = {
    require(avro.getType == AvroSchema.Type.RECORD, "Can only convert avro records to eel schemas")
    val cols = avro.getFields.asScala.map(toColumn)
    Schema(cols.toList)
  }

  def toSchemaType(schema: AvroSchema): SchemaType = {
    schema.getType match {
      case AvroSchema.Type.BOOLEAN => SchemaType.Boolean
      case AvroSchema.Type.DOUBLE => SchemaType.Double
      case AvroSchema.Type.ENUM => SchemaType.String
      case AvroSchema.Type.FIXED => SchemaType.String
      case AvroSchema.Type.FLOAT => SchemaType.Float
      case AvroSchema.Type.INT => SchemaType.Int
      case AvroSchema.Type.LONG => SchemaType.Long
      case other =>
        logger.warn(s"Unrecognized avro type $other; defaulting to string")
        SchemaType.String
    }
  }

  def toColumn(field: AvroSchema.Field): Column = {
    field.schema.getType match {
      case AvroSchema.Type.UNION =>
        val schemaType = field.schema.getTypes.asScala.filter(_.getType != AvroSchema.Type.NULL).collectFirst {
          case schema => toSchemaType(schema)
        }.getOrElse(sys.error("Union types must define a non null type"))
        Column(field.name, schemaType, true)
      case _ =>
        val schemaType = toSchemaType(field.schema)
        Column(field.name, schemaType, false)
    }
  }

  def toAvroField(column: Column): AvroSchema.Field = {
    val schema = column.`type` match {
      case SchemaType.String => SchemaBuilder.builder().stringType()
      case SchemaType.Int => SchemaBuilder.builder().intType()
      case SchemaType.Short => SchemaBuilder.builder().intType()
      case SchemaType.Boolean => AvroSchema.create(AvroSchema.Type.BOOLEAN)
      case SchemaType.Double => AvroSchema.create(AvroSchema.Type.DOUBLE)
      case SchemaType.Float => AvroSchema.create(AvroSchema.Type.FLOAT)
      case SchemaType.Long => AvroSchema.create(AvroSchema.Type.LONG)
      case SchemaType.BigInt => AvroSchema.create(AvroSchema.Type.LONG)
      case other =>
        logger.warn(s"Unknown column type ${column.name}= ${column.`type`}; defaulting to string")
        AvroSchema.create(AvroSchema.Type.STRING)
    }

    if (column.nullable) {
      val union = SchemaBuilder.unionOf().nullType().and().`type`(schema).endUnion()
      new AvroSchema.Field(column.name, union, null, NullNode.getInstance())
    } else {
      new AvroSchema.Field(column.name, schema, null, null)
    }
  }
}
