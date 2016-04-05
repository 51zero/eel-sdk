package io.eels

data class Column(val name: String,
                  val `type`: ColumnType,
                  val nullable: Boolean,
                  val precision: Int = 0,
                  val scale: Int = 0,
                  val signed: Boolean = true,
                  val comment: String = "") {
  /**
   * Creates a lowercase version of this column
   */
  fun toLowerCase(): Column = copy(name = name.toLowerCase())
}

enum class ColumnType {
  BigInt,
  Binary,
  Boolean,
  Date,
  Decimal,
  Double,
  Float,
  Int,
  Long,
  Short,
  String,
  Timestamp,
  Unsupported
}