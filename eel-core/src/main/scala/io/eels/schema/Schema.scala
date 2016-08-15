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
case class Schema(fields: List[Field]) {

  require(fields.map(_.name).distinct.length == fields.size, "Schema cannot have duplicated field name")
  require(fields.nonEmpty, "Schema cannot be empty")

  def apply(name: String): Option[Field] = fields.find(_.name == name)

  def indexOf(field: Field): Int = indexOf(field.name, true)
  def indexOf(field: Field, caseSensitive: Boolean): Int = indexOf(field.name, caseSensitive)

  def indexOf(fieldName: String): Int = indexOf(fieldName, true)
  def indexOf(fieldName: String, caseSensitive: Boolean): Int = {
    if (caseSensitive) {
      fields.indexWhere(_.name == fieldName)
    } else {
      fields.indexWhere(_.name.equalsIgnoreCase(fieldName))
    }
  }

  def projection(fieldNames: Seq[String]): Schema = Schema(
    fieldNames.flatMap { name =>
      field(name)
    }.toList
  )

  def field(name: String): Option[Field] = fields.find(_.name == name)

  def toLowerCase(): Schema = copy(fields = fields.map(_.toLowerCase()))

  def fieldNames(): List[String] = fields.map(_.name)

  def addField(name: String): Schema = addField(Field(name))

  def addField(field: Field): Schema = {
    require(!fieldNames().contains(field.name), s"Field ${field.name} already exists")
    copy(fields = fields :+ field)
  }

  def contains(fieldName: String, caseSensitive: Boolean = true): Boolean = {
    def contains(fields: Seq[Field]): Boolean = fields.exists { field =>
      if (caseSensitive) fieldName == field.name else fieldName equalsIgnoreCase field.name
    } || fields.filter { field =>
      field.`type` == FieldType.Struct
    }.exists { field =>
      contains(field.fields)
    }
    contains(fields)
  }

  def stripFromFieldNames(chars: Seq[Char]): Schema = {
    def strip(name: String): String = chars.foldLeft(name) { (name, char) => name.replace(char.toString, "") }
    Schema(fields.map { field =>
      field.copy(name = strip(field.name))
    })
  }

  def addFieldIfNotExists(name: String): Schema = if (fieldNames().contains(name)) this else addField(Field(name))
  def addFieldIfNotExists(field: Field): Schema = if (fieldNames().contains(field.name)) this else addField(field)

  def updateFieldType(fieldName: String, fieldType: FieldType): Schema = copy(fields = fields.map { field =>
    if (field.name == fieldName) field.copy(`type` = fieldType)
    else field
  })

  def removeFields(first: String, rest: String*): Schema = removeFields(first +: rest)
  def removeFields(names: Seq[String]): Schema = copy(fields = fields.filterNot { field =>
    names.contains(field.name)
  })

  def removeField(name: String, caseSensitive: Boolean = true): Schema = {
    copy(fields = fields.filterNot { field =>
      if (caseSensitive) field.name == name else field.name.equalsIgnoreCase(name)
    })
  }

  def size(): Int = fields.size

  def join(other: Schema): Schema = {
    require(
      fields.map(_.name).intersect(other.fields.map(_.name)).isEmpty,
      "Cannot join two schemas which have duplicated field names"
    )
    Schema(fields ++ other.fields)
  }

  def replaceField(sourceFieldName: String, targetField: Field): Schema = Schema(
    fields.map {
      case field if field.name == sourceFieldName => targetField
      case field => field
    }
  )

  def renameField(from: String, to: String): Schema = Schema(fields.map { field =>
    if (field.name == from) field.copy(name = to) else field
  })

  def show(): String = {
    "Schema\n" + fields.map { field =>
      val signedString = if (field.signed) "signed" else "unsigned"
      val nullString = if (field.nullable) "nullable" else "not nullable"
      val partitionString = if (field.partition) "partition" else ""
      s"- ${field.name} [${field.`type`} $nullString scale=${field.scale.value} precision=${field.precision.value} $signedString $partitionString]"
    }.mkString("\n")
  }

  def ddl(table: String): String = {
    s"CREATE TABLE $table " + fields.map { field => field.name + " " + field.`type` }.mkString("(", ", ", ")")
  }
}

object Schema {
  def apply(first: Field, rest: Field*): Schema = apply((first +: rest).toList)
  def apply(first: String, rest: String*): Schema = apply((first +: rest).map(name => Field(name)).toList)

  //    def from[T <: Product : TypeTag : ClassTag]: Schema =
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

//object SchemaFn {
//  def toColumnType(clz: Class[_]): ColumnType =
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
