package io.eels

data class Column(val name: String,
                  val `type`: ColumnType = ColumnType.String,
                  val nullable: Boolean = true,
                  val precision: Int = 0,
                  val scale: Int = 0,
                  val signed: Boolean = true,
                  val comment: String = "") {

  /**
   * Creates a lowercase version of this column
   */
  fun toLowerCase(): Column = copy(name = name.toLowerCase())
}

data class Row(val schema: Schema, val values: List<Any?>) {

  init {
    require(schema.size() == values.size, { "Row should have a value for each column (${schema.columns.size} columns=${schema.columns.joinToString { "," }}, ${values.size} values=${values.joinToString { "," }})" })
  }

  override fun toString(): String {
    return schema.columnNames().zip(values).map { it ->
      "${it.first} = ${if (it.second == null) "" else it.second.toString()}"
    }.joinToString ("[", ",", "]")
  }

  fun get(k: Int): Any? = values[k]

  fun get(name: String, caseInsensitive: Boolean = false): Any? {
    val index = schema.indexOf(name, caseInsensitive)
    return values[index]
  }

  companion object {
    val poisonPill = Row(Schema(Column("a")), listOf(object : Any() {}))
  }

  fun size(): Int = values.size
}


data class Database(val name: String, val tables: List<Table>)

data class Table(val name: String,
                 val columns: Column,
                 val partitionKeys: List<PartitionKey>,
                 val props: Map<String, String>)

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