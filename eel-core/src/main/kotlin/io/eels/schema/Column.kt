package io.eels.schema

data class Column(val name: String,
                  val `type`: ColumnType = ColumnType.String,
                  val nullable: Boolean = true,
                  val precision: Precision = Precision(0),
                  val scale: Scale = Scale(0),
                  val signed: Boolean = true,
                  val comment: String = "") {
  // Creates a lowercase version of this column
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

data class Precision(val value: Int)
data class Scale(val value: Int)