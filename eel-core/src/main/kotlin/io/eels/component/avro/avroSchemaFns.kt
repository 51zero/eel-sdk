package io.eels.component.avro

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import org.apache.avro.SchemaBuilder
import org.codehaus.jackson.node.NullNode

fun toAvroSchema(schema: Schema, caseSensitive: Boolean = true, name: String = "row", namespace: String = "namespace"): org.apache.avro.Schema {
  val avroFields = schema.fields.map { toAvroField(it, caseSensitive) }
  val avroSchema = org.apache.avro.Schema.createRecord(name, null, namespace, false)
  avroSchema.fields = avroFields
  return avroSchema
}

fun fromAvroSchema(avro: org.apache.avro.Schema): Schema {
  require(avro.type == org.apache.avro.Schema.Type.RECORD, { "Can only convert avro records to eel schemas" })
  val cols = avro.fields.map(::fromAvroField)
  return Schema(cols)
}

fun toFieldType(schema: org.apache.avro.Schema): FieldType = when (schema.type) {
  org.apache.avro.Schema.Type.BOOLEAN -> FieldType.Boolean
  org.apache.avro.Schema.Type.DOUBLE -> FieldType.Double
  org.apache.avro.Schema.Type.ENUM -> FieldType.String
  org.apache.avro.Schema.Type.FIXED -> FieldType.String
  org.apache.avro.Schema.Type.FLOAT -> FieldType.Float
  org.apache.avro.Schema.Type.INT -> FieldType.Int
  org.apache.avro.Schema.Type.LONG -> FieldType.Long
  else -> {
    //LoggerFactory.getLogger(ja).warn("Unrecognized avro type ${schema.type}; defaulting to string")
    FieldType.String
  }
}

fun fromAvroField(field: org.apache.avro.Schema.Field): Field = when (field.schema().type) {
  org.apache.avro.Schema.Type.UNION -> {
    val schema = field.schema().types.find { it.type != org.apache.avro.Schema.Type.NULL }
    val fieldType = toFieldType(schema!!)
    Field(field.name(), fieldType, true)
  }
  else -> {
    val schemaType = toFieldType(field.schema())
    Field(field.name(), schemaType, false)
  }
}

fun toAvroField(field: Field, caseSensitive: Boolean = true): org.apache.avro.Schema.Field {

  val schema = when (field.type) {
    FieldType.String -> SchemaBuilder.builder().stringType()
    FieldType.Int -> SchemaBuilder.builder().intType()
    FieldType.Short -> SchemaBuilder.builder().intType()
    FieldType.Boolean -> SchemaBuilder.builder().booleanType()
    FieldType.Double -> SchemaBuilder.builder().doubleType()
    FieldType.Float -> SchemaBuilder.builder().floatType()
    FieldType.Long -> SchemaBuilder.builder().longType()
    FieldType.BigInt -> SchemaBuilder.builder().longType()
    else -> {
      //LoggerFactory.getLogger(this.javaClass).warn("Unknown column type ${column.name}= ${column.`type`}; defaulting to string")
      org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)
    }
  }

  val name = if (caseSensitive) field.name else field.name.toLowerCase()

  return if (field.nullable) {
    val union = SchemaBuilder.unionOf().nullType().and().`type`(schema).endUnion()
    org.apache.avro.Schema.Field(name, union, null, NullNode.getInstance())
  } else {
    org.apache.avro.Schema.Field(name, schema, null, null)
  }
}