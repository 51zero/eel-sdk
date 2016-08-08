package io.eels.component.avro

import com.sksamuel.exts.Logging
import io.eels.schema.{Field, FieldType, Schema}
import org.apache.avro.SchemaBuilder
import org.codehaus.jackson.node.NullNode
import scala.collection.JavaConverters._

object AvroSchemaFns extends Logging {

  def toAvroSchema(schema: Schema,
                   caseSensitive: Boolean = true,
                   name: String = "row",
                   namespace: String = "namespace"): org.apache.avro.Schema = {

    def toAvroField(field: Field, caseSensitive: Boolean = true): org.apache.avro.Schema.Field = {

      val avroSchema = field.`type` match {
        case FieldType.String => SchemaBuilder.builder().stringType()
        case FieldType.Int => SchemaBuilder.builder().intType()
        case FieldType.Short => SchemaBuilder.builder().intType()
        case FieldType.Boolean => SchemaBuilder.builder().booleanType()
        case FieldType.Double => SchemaBuilder.builder().doubleType()
        case FieldType.Float => SchemaBuilder.builder().floatType()
        case FieldType.Long => SchemaBuilder.builder().longType()
        case FieldType.BigInt => SchemaBuilder.builder().longType()
        case _ =>
          logger.warn(s"Unknown field type ${field.name}=${field.`type`}; defaulting to string")
          org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)
      }

      val fieldName = if (caseSensitive) field.name else field.name.toLowerCase()

      if (field.nullable) {
        val union = SchemaBuilder.unionOf().nullType().and().`type`(avroSchema).endUnion()
        new org.apache.avro.Schema.Field(fieldName, union, null, NullNode.getInstance())
      } else {
        new org.apache.avro.Schema.Field(fieldName, avroSchema, null, null)
      }
    }


    val avroFields = schema.fields.map { field => toAvroField(field, caseSensitive) }
    val avroSchema = org.apache.avro.Schema.createRecord(name, null, namespace, false)
    avroSchema.setFields(avroFields.asJava)
    avroSchema
  }

  def fromAvroSchema(avro: org.apache.avro.Schema): io.eels.schema.Schema = {

    def fromAvroField(field: org.apache.avro.Schema.Field): Field = {

      def toFieldType(schema: org.apache.avro.Schema): FieldType = schema.getType match {
        case org.apache.avro.Schema.Type.BOOLEAN => FieldType.Boolean
        case org.apache.avro.Schema.Type.DOUBLE => FieldType.Double
        case org.apache.avro.Schema.Type.ENUM => FieldType.String
        case org.apache.avro.Schema.Type.FIXED => FieldType.String
        case org.apache.avro.Schema.Type.FLOAT => FieldType.Float
        case org.apache.avro.Schema.Type.INT => FieldType.Int
        case org.apache.avro.Schema.Type.LONG => FieldType.Long
        case _ =>
          logger.warn(s"Unrecognized avro type ${schema.getType}; defaulting to string")
          FieldType.String
      }

      field.schema().getType match {
        case org.apache.avro.Schema.Type.UNION =>
          val schema = field.schema().getTypes.asScala.find(_.getType != org.apache.avro.Schema.Type.NULL).get
          val fieldType = toFieldType(schema)
          Field(field.name(), fieldType, true)
        case _ =>
          val schemaType = toFieldType(field.schema())
          Field(field.name(), schemaType, false)
      }
    }

    require(avro.getType == org.apache.avro.Schema.Type.RECORD, "Can only convert avro records to eel schemas")
    val cols = avro.getFields.asScala.map(fromAvroField)
    io.eels.schema.Schema(cols.toList)
  }
}