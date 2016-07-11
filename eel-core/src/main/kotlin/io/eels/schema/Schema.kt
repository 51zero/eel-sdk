package io.eels.schema

/**
 * An eel schema contains:
 *
 * - Tables: Which contain fields
 * - Fields: Which can be set as a partition.
 *
 * Not all components can support partitions. In those cases the partitions are ignored.
 *
 */
data class Schema(val fields: List<Field>) {

  constructor(vararg fields: Field) : this(fields.asList())

  init {
    require(fields.map { it.name }.distinct().size == fields.size, { "Schema cannot have duplicated field name" })
    require(fields.size > 0, { "Schema cannot be empty" })
  }

  fun apply(name: String): Field? = fields.find { it.name == name }

  fun indexOf(field: Field): Int = indexOf(field.name, true)
  fun indexOf(field: Field, caseSensitive: Boolean): Int = indexOf(field.name, caseSensitive)

  fun indexOf(fieldName: String): Int = indexOf(fieldName, true)
  fun indexOf(fieldName: String, caseSensitive: Boolean): Int =
      fields.indexOfFirst { fieldName.equals(it.name, !caseSensitive) }

  fun toLowerCase(): Schema = copy(fields = fields.map { it.copy(name = it.name.toLowerCase()) })

  fun fieldNames(): List<String> = fields.map { it.name }

  fun addField(name: String): Schema = addField(Field(name))

  fun addField(col: Field): Schema {
    require(!fieldNames().contains(col.name), { "Field ${col.name} already exist" })
    return copy(fields + col)
  }

  fun contains(fieldName: String, caseSensitive: Boolean = true): Boolean {
    fun contains(fields: List<Field>): Boolean = fields.any {
      fieldName.equals(it.name, !caseSensitive)
    } || fields.filter { it.type == FieldType.Struct }.any { contains(it.fields) }
    return contains(fields)
  }

  fun stripFromFieldNames(chars: List<Char>): Schema {
    fun strip(name: String): String = chars.fold(name, { str, char -> str.replace(char.toString(), "") })
    return Schema(fields.map { it.copy(name = strip(it.name)) })
  }

  fun addFieldIfNotExists(name: String): Schema = if (fieldNames().contains(name)) this else addField(Field(name))
  fun addFieldIfNotExists(field: Field): Schema = if (fieldNames().contains(field.name)) this else addField(field)

  fun updateFieldType(fieldName: String, FieldType: FieldType): Schema = Schema(
      fields.map {
        if (it.name == fieldName) it.copy(type = FieldType)
        else it
      }
  )

  fun removeFields(vararg names: String): Schema = removeFields(names.asList())
  fun removeFields(names: List<String>): Schema = copy(fields = fields.filterNot { names.contains(it.name) })

  fun removeField(name: String, caseSensitive: Boolean = true): Schema {
    return copy(fields = fields.filterNot {
      if (caseSensitive) it.name == name else it.name.equals(name, true)
    })
  }

  fun size(): Int = fields.size

  fun join(other: Schema): Schema {
    require(
        fields.map { it.name }.intersect(other.fields.map { it.name }).isEmpty(),
        { "Cannot join two schemas which have duplicated field names" }
    )
    return Schema(fields + other.fields)
  }

  fun replaceField(name: String, field: Field): Schema = Schema(fields.map {
    if (it.name == name) field else it
  })

  fun renameField(from: String, to: String): Schema = Schema(fields.map {
    if (it.name == from) it.copy(name = to) else it
  })

  fun show(): String {
    return "Schema\n" + fields.map {
      val signedString = if (it.signed) "signed" else "unsigned"
      val nullString = if (it.nullable) "nullable" else "not nullable"
      val partitionString = if (it.partition) "partition" else ""
      "- ${it.name} [${it.`type`} $nullString scale=${it.scale.value} precision=${it.precision.value} $signedString $partitionString]"
    }.joinToString("\n")
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
