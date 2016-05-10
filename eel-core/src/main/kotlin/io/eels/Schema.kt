package io.eels

data class Schema(val columns: List<Column>) {

  constructor(vararg columns: Column) : this(columns.asList())

  init {
    require(columns.map { it.name }.distinct().size == columns.size, { "Schema cannot have duplicated column name" })
    require(columns.size > 0, { "Schema cannot be empty" })
  }

  fun apply(name: String): Column? = columns.find { it.name == name }

  fun indexOf(column: Column): Int = indexOf(column.name, true)
  fun indexOf(column: Column, caseSensitive: Boolean): Int = indexOf(column.name, caseSensitive)

  fun indexOf(columnName: String): Int = indexOf(columnName, true)
  fun indexOf(columnName: String, caseSensitive: Boolean): Int = columns.indexOfFirst { it ->
    if (caseSensitive) columnName == it.name else columnName.equals(it.name, true)
  }

  fun toLowerCase(): Schema = copy(columns = columns.map { it.copy(name = it.name.toLowerCase()) })

  fun columnNames(): List<String> = columns.map { it.name }

  fun addColumn(name: String): Schema = addColumn(Column(name))

  fun addColumn(col: Column): Schema {
    require(!columnNames().contains(col.name), { "Column ${col.name} already exist" })
    return copy(columns + col)
  }

  fun contains(columnName: String): Boolean = indexOf(columnName) >= 0

  fun stripFromColumnName(chars: List<Char>): Schema {
    fun strip(name: String): String = chars.fold(name, { str, char -> str.replace(char.toString(), "") })
    return Schema(columns.map { it.copy(name = strip(it.name)) })
  }

  fun addColumnIfNotExists(col: Column): Schema = if (columnNames().contains(col.name)) this else addColumn(col)

  fun updateColumnType(columnName: String, ColumnType: ColumnType): Schema {
    return Schema(
        columns.map {
          if (it.name == columnName) it.copy(type = ColumnType)
          else it
        }
    )
  }

  fun removeColumn(name: String, caseSensitive: Boolean = true): Schema {
    return copy(columns = columns.filterNot {
      if (caseSensitive) it.name == name else it.name.equals(name, true)
    })
  }

  fun size(): Int = columns.size

  fun removeColumns(names: List<String>): Schema = copy(columns = columns.filterNot { names.contains(it.name) })

  fun join(other: Schema): Schema {
    require(
        columns.map { it.name }.intersect(other.columns.map { it.name }).isEmpty(),
        { "Cannot join two frames which have duplicated column name" }
    )
    return Schema(columns + other.columns)
  }

  fun updateColumn(column: Column): Schema = Schema(columns.map {
    if (it.name == column.name) column else it
  })

  fun renameColumn(from: String, to: String): Schema = Schema(columns.map {
    if (it.name == from) it.copy(name = to) else it
  })

  fun print(): String {
    return columns.map { column ->
      val signedString = if (column.signed) "signed" else "unsigned"
      val nullString = if (column.nullable) "null" else "not null"
      "- ${column.name} [${column.`type`} $nullString scale=${column.scale} precision=${column.precision} $signedString]"
    }.joinToString ("\n")
  }

  fun ddl(table: String): String {
    return "CREATE TABLE $table " + columns.map { it.name + " " + it.`type` }.joinToString("(", ", ", ")")
  }

  companion object {
    fun apply(vararg columns: Column): Schema = Schema(columns.toList())
    //  fun apply(first: String, vararg rest: String): Schema = apply(first + rest)

    //    fun apply(strs: Seq<String>): io.eels.Schema = Schema(strs.map(Column.apply).toList)

    //    fun from[T <: Product : TypeTag : ClassTag]: Schema =
    //    {
    //      val columns = typeOf[T].declarations.collect {
    //        case m: MethodSymbol if m.isCaseAccessor =>
    //        val javaClass = implicitly[TypeTag[T]].mirror.runtimeClass(m.returnType.typeSymbol.asClass)
    //        val ColumnType = SchemaFn.toColumnType(javaClass)
    //        Column(m.name.toString, ColumnType, true)
    //      }
    //      Schema(columns.toList)
    //    }
  }
}

//object SchemaFn {
//  fun toColumnType(clz: Class[_]): ColumnType =
//  {
//    val intClass = classOf[Int]
//    val floatClass = classOf[Float]
//    val stringClass = classOf<String>
//    val charClass = classOf[Char]
//    val bigIntClass = classOf[BigInt]
//    val booleanClass = classOf[Boolean]
//    val doubleClass = classOf[Double]
//    val bigdecimalClass = classOf[BigDecimal]
//    val longClass = classOf[Long]
//    clz match {
//      case `intClass` => ColumnType.Int
//          case `floatClass` => ColumnType.Float
//          case `stringClass` => ColumnType.String
//          case `charClass` => ColumnType.String
//          case `bigIntClass` => ColumnType.BigInt
//          case `booleanClass` => ColumnType.Boolean
//          case `doubleClass` => ColumnType.Double
//          case `longClass` => ColumnType.Long
//          case `bigdecimalClass` => ColumnType.Decimal
//          case _ => sys.error("Can not map $clz to ColumnType value.")
//    }
//  }
//}
