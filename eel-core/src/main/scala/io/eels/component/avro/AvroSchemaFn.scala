package io.eels.component.avro

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Column, Schema, SchemaType}
import org.apache.avro.{Schema => AvroSchema, SchemaBuilder}
import org.codehaus.jackson.node.NullNode

import scala.collection.JavaConverters._

object AvroSchemaFn extends StrictLogging {

  def toAvro(fs: Schema, caseSensitive: Boolean = true, name: String = "row", namespace: String = "namespace"): AvroSchema = {
    val avro = AvroSchema.createRecord(name, null, namespace, false)
    val fields = fs.columns.map(toAvroField(_, caseSensitive))
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

  def toAvroField(column: Column, caseSensitive: Boolean = true): AvroSchema.Field = {

    val schema = column.`type` match {
      case SchemaType.String => SchemaBuilder.builder().stringType()
      case SchemaType.Int => SchemaBuilder.builder().intType()
      case SchemaType.Short => SchemaBuilder.builder().intType()
      case SchemaType.Boolean => SchemaBuilder.builder().booleanType()
      case SchemaType.Double => SchemaBuilder.builder().doubleType()
      case SchemaType.Float => SchemaBuilder.builder().floatType()
      case SchemaType.Long => SchemaBuilder.builder().longType()
      case SchemaType.BigInt => SchemaBuilder.builder().longType()
      case other =>
        logger.warn(s"Unknown column type ${column.name}= ${column.`type`}; defaulting to string")
        AvroSchema.create(AvroSchema.Type.STRING)
    }

    val name = if (caseSensitive) column.name else column.name.toLowerCase

    if (column.nullable) {
      val union = SchemaBuilder.unionOf().nullType().and().`type`(schema).endUnion()
      new AvroSchema.Field(name, union, null, NullNode.getInstance())
    } else {
      new AvroSchema.Field(name, schema, null, null)
    }
  }
}
