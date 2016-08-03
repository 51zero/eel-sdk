package io.eels.component.avro

import io.eels.schema.Field
import io.eels.schema.FieldType
import org.apache.avro.Schema

fun fromAvroSchema(avro: Schema): io.eels.schema.Schema {

  fun fromAvroField(field: Schema.Field): Field {

    fun toFieldType(schema: Schema): FieldType = when (schema.type) {
      Schema.Type.BOOLEAN -> FieldType.Boolean
      Schema.Type.DOUBLE -> FieldType.Double
      Schema.Type.ENUM -> FieldType.String
      Schema.Type.FIXED -> FieldType.String
      Schema.Type.FLOAT -> FieldType.Float
      Schema.Type.INT -> FieldType.Int
      Schema.Type.LONG -> FieldType.Long
      else -> {
        //LoggerFactory.getLogger(ja).warn("Unrecognized avro type ${schema.type}; defaulting to string")
        FieldType.String
      }
    }

    return when (field.schema().type) {
      Schema.Type.UNION -> {
        val schema = field.schema().types.find { it.type != Schema.Type.NULL }
        val fieldType = toFieldType(schema!!)
        Field(field.name(), fieldType, true)
      }
      else -> {
        val schemaType = toFieldType(field.schema())
        Field(field.name(), schemaType, false)
      }
    }
  }

  require(avro.type == Schema.Type.RECORD, { "Can only convert avro records to eel schemas" })
  val cols = avro.fields.map(::fromAvroField)
  return io.eels.schema.Schema(cols)
}