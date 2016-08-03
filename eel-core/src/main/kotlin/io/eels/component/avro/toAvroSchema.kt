package io.eels.component.avro

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import org.apache.avro.SchemaBuilder
import org.codehaus.jackson.node.NullNode

fun toAvroSchema(schema: Schema,
                 caseSensitive: Boolean = true,
                 name: String = "row",
                 namespace: String = "namespace"): org.apache.avro.Schema {

  fun toAvroField(field: Field, caseSensitive: Boolean = true): org.apache.avro.Schema.Field {

    val avroSchema = when (field.type) {
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

    val fieldName = if (caseSensitive) field.name else field.name.toLowerCase()

    return if (field.nullable) {
      val union = SchemaBuilder.unionOf().nullType().and().`type`(avroSchema).endUnion()
      org.apache.avro.Schema.Field(fieldName, union, null, NullNode.getInstance())
    } else {
      org.apache.avro.Schema.Field(fieldName, avroSchema, null, null)
    }
  }

  val avroFields = schema.fields.map { toAvroField(it, caseSensitive) }
  val avroSchema = org.apache.avro.Schema.createRecord(name, null, namespace, false)
  avroSchema.fields = avroFields
  return avroSchema
}

