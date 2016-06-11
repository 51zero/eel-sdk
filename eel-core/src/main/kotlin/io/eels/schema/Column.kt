package io.eels.schema

import io.eels.schema.ColumnType

data class Column(val name: String,
                  val `type`: ColumnType = ColumnType.String,
                  val nullable: Boolean = true,
                  val precision: Int = 0,
                  val scale: Int = 0,
                  val signed: Boolean = true,
                  val comment: String = "") {
  // Creates a lowercase version of this column
  fun toLowerCase(): Column = copy(name = name.toLowerCase())
}