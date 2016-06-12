package io.eels.component

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Schema
import io.eels.util.Option
import io.eels.util.getOrElse
import io.eels.util.orElse

interface SchemaInferrer {

  fun schemaOf(headers: List<String>): Schema

  companion object {
    operator fun invoke(default: FieldType, vararg rules: SchemaRule): SchemaInferrer = object : SchemaInferrer {
      override fun schemaOf(headers: List<String>): Schema {
        val columns = headers.map {
          rules.fold(Option.none<Field>(), { columnType, rule ->
            columnType.orElse(rule(it))
          }).getOrElse(Field(it, default, true))
        }
        return Schema(columns.toList())
      }
    }
  }
}

object StringInferrer : SchemaInferrer {
  override fun schemaOf(headers: List<String>): Schema = Schema(headers.map { Field(it, FieldType.String, true) }.toList())
}

data class SchemaRule(val pattern: String, val fieldType: FieldType, val nullable: Boolean = true) {
  operator fun invoke(header: String): Option<Field> {
    return if (header.matches(pattern.toRegex())) Option.Some(Field(header, fieldType, nullable)) else Option.None
  }
}