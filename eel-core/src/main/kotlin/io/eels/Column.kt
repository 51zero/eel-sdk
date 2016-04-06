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

  companion object {

  }
}

data class Row(val schema: Schema, val values: List<Any?>) {

  override fun toString(): String {
    return schema.columnNames().zip(values).map { it ->
      "${it.first} = ${if (it.second == null) "" else it.second.toString()}"
    }.joinToString ("[", ",", "]")
  }

  companion object {
    val poisonPill = Row(Schema(listOf()), listOf(object : Any() {}))
  }
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