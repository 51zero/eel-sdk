package io.eels.component.avro

import io.eels.schema.Column
import io.eels.schema.ColumnType
import io.eels.schema.Schema
import org.apache.avro.SchemaBuilder
import org.codehaus.jackson.node.NullNode

fun schemaToAvroSchema(schema: Schema, caseSensitive: Boolean = true, name: String = "row", namespace: String = "namespace"): org.apache.avro.Schema {
  val avroFields = schema.columns.map { toAvroField(it, caseSensitive) }
  val avroSchema = org.apache.avro.Schema.createRecord(name, null, namespace, false)
  avroSchema.fields = avroFields
  return avroSchema
}

fun avroSchemaToSchema(avro: org.apache.avro.Schema): Schema {
  require(avro.type == org.apache.avro.Schema.Type.RECORD, { "Can only convert avro records to eel schemas" })
  val cols = avro.fields.map(::toColumn)
  return Schema(cols)
}

fun toSchemaType(schema: org.apache.avro.Schema): ColumnType = when (schema.type) {
  org.apache.avro.Schema.Type.BOOLEAN -> ColumnType.Boolean
  org.apache.avro.Schema.Type.DOUBLE -> ColumnType.Double
  org.apache.avro.Schema.Type.ENUM -> ColumnType.String
  org.apache.avro.Schema.Type.FIXED -> ColumnType.String
  org.apache.avro.Schema.Type.FLOAT -> ColumnType.Float
  org.apache.avro.Schema.Type.INT -> ColumnType.Int
  org.apache.avro.Schema.Type.LONG -> ColumnType.Long
  else -> {
    //LoggerFactory.getLogger(ja).warn("Unrecognized avro type ${schema.type}; defaulting to string")
    ColumnType.String
  }
}

fun toColumn(field: org.apache.avro.Schema.Field): Column = when (field.schema().type) {
  org.apache.avro.Schema.Type.UNION -> {
    val schema = field.schema().types.find { it.type != org.apache.avro.Schema.Type.NULL }
    val fieldType = toSchemaType(schema!!)
    Column(field.name(), fieldType, true)
  }
  else -> {
    val schemaType = toSchemaType(field.schema())
    Column(field.name(), schemaType, false)
  }
}

fun toAvroField(column: Column, caseSensitive: Boolean = true): org.apache.avro.Schema.Field {

  val schema = when (column.type) {
    ColumnType.String -> SchemaBuilder.builder().stringType()
    ColumnType.Int -> SchemaBuilder.builder().intType()
    ColumnType.Short -> SchemaBuilder.builder().intType()
    ColumnType.Boolean -> SchemaBuilder.builder().booleanType()
    ColumnType.Double -> SchemaBuilder.builder().doubleType()
    ColumnType.Float -> SchemaBuilder.builder().floatType()
    ColumnType.Long -> SchemaBuilder.builder().longType()
    ColumnType.BigInt -> SchemaBuilder.builder().longType()
    else -> {
      //LoggerFactory.getLogger(this.javaClass).warn("Unknown column type ${column.name}= ${column.`type`}; defaulting to string")
      org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)
    }
  }

  val name = if (caseSensitive) column.name else column.name.toLowerCase()

  return if (column.nullable) {
    val union = SchemaBuilder.unionOf().nullType().and().`type`(schema).endUnion()
    org.apache.avro.Schema.Field(name, union, null, NullNode.getInstance())
  } else {
    org.apache.avro.Schema.Field(name, schema, null, null)
  }
}