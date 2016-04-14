package io.eels.component

import io.eels.Column
import io.eels.ColumnType
import io.eels.Option
import io.eels.Schema
import io.eels.getOrElse
import io.eels.orElse

interface SchemaInferrer {

  fun schemaOf(headers: List<String>): Schema

  companion object {
    operator fun invoke(default: ColumnType, vararg rules: SchemaRule): SchemaInferrer = object : SchemaInferrer {
      override fun schemaOf(headers: List<String>): Schema {
        val columns = headers.map {
          rules.fold(Option.none<Column>(), { columnType, rule ->
            columnType.orElse(rule(it))
          }).getOrElse(Column(it, default, true))
        }
        return Schema(columns.toList())
      }
    }
  }
}

object StringInferrer : SchemaInferrer {
  override fun schemaOf(headers: List<String>): Schema = Schema(headers.map { Column(it, ColumnType.String, true) }.toList())
}

data class SchemaRule(val pattern: String, val columnType: ColumnType, val nullable: Boolean = true) {
  operator fun invoke(header: String): Option<Column> {
    return if (header.matches(pattern.toRegex())) Option.Some(Column(header, columnType, nullable)) else Option.None
  }
}