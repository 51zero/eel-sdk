package io.eels.schema

// todo rename columns to fields
data class Schema(val fields: List<Field>) {

  constructor(vararg fields: Field) : this(fields.asList())

  init {
    require(fields.map { it.name }.distinct().size == fields.size, { "Schema cannot have duplicated column name" })
    require(fields.size > 0, { "Schema cannot be empty" })
  }

  fun apply(name: String): Field? = fields.find { it.name == name }

  fun indexOf(field: Field): Int = indexOf(field.name, true)
  fun indexOf(field: Field, caseSensitive: Boolean): Int = indexOf(field.name, caseSensitive)

  fun indexOf(columnName: String): Int = indexOf(columnName, true)
  fun indexOf(columnName: String, caseSensitive: Boolean): Int = fields.indexOfFirst { it ->
    if (caseSensitive) columnName == it.name else columnName.equals(it.name, true)
  }

  fun toLowerCase(): Schema = copy(fields = fields.map { it.copy(name = it.name.toLowerCase()) })

  fun columnNames(): List<String> = fields.map { it.name }

  fun addColumn(name: String): Schema = addColumn(Field(name))

  fun addColumn(col: Field): Schema {
    require(!columnNames().contains(col.name), { "Column ${col.name} already exist" })
    return copy(fields + col)
  }

  fun contains(columnName: String): Boolean = indexOf(columnName) >= 0

  fun stripFromColumnName(chars: List<Char>): Schema {
    fun strip(name: String): String = chars.fold(name, { str, char -> str.replace(char.toString(), "") })
    return Schema(fields.map { it.copy(name = strip(it.name)) })
  }

  fun addColumnIfNotExists(col: Field): Schema = if (columnNames().contains(col.name)) this else addColumn(col)

  fun updateColumnType(columnName: String, FieldType: FieldType): Schema {
    return Schema(
        fields.map {
          if (it.name == columnName) it.copy(type = FieldType)
          else it
        }
    )
  }

  fun removeColumn(name: String, caseSensitive: Boolean = true): Schema {
    return copy(fields = fields.filterNot {
      if (caseSensitive) it.name == name else it.name.equals(name, true)
    })
  }

  fun size(): Int = fields.size

  fun removeColumns(names: List<String>): Schema = copy(fields = fields.filterNot { names.contains(it.name) })

  fun join(other: Schema): Schema {
    require(
        fields.map { it.name }.intersect(other.fields.map { it.name }).isEmpty(),
        { "Cannot join two frames which have duplicated column name" }
    )
    return Schema(fields + other.fields)
  }

  fun updateColumn(field: Field): Schema = Schema(fields.map {
    if (it.name == field.name) field else it
  })

  fun renameColumn(from: String, to: String): Schema = Schema(fields.map {
    if (it.name == from) it.copy(name = to) else it
  })

  fun show(): String {
    return "Schema\n" + fields.map { column ->
      val signedString = if (column.signed) "signed" else "unsigned"
      val nullString = if (column.nullable) "nullable" else "not nullable"
      "- ${column.name} [${column.`type`} $nullString scale=${column.scale.value} precision=${column.precision.value} $signedString]"
    }.joinToString ("\n")
  }

  fun ddl(table: String): String {
    return "CREATE TABLE $table " + fields.map { it.name + " " + it.`type` }.joinToString("(", ", ", ")")
  }

  companion object {
    fun apply(vararg fields: Field): Schema = Schema(fields.toList())
    //  fun apply(first: String, vararg rest: String): Schema = apply(first + rest)

    //    fun apply(strs: Seq<String>): io.eels.schema.Schema = Schema(strs.map(Column.apply).toList)

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
