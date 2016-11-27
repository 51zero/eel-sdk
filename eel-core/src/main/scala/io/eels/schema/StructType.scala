package io.eels.schema

case class StructType(fields: List[Field]) extends DataType {
  require(fields.map(_.name).distinct.length == fields.size, "StructType cannot have duplicated field names")
  require(fields.nonEmpty, "StructType cannot be empty")

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

  def projection(fieldNames: Seq[String]): StructType = StructType(
    fieldNames.flatMap { name =>
      field(name)
    }.toList
  )

  def replaceFieldType(from: DataType, to: DataType): StructType = {
    StructType(fields.map {
      case field if field.dataType == from => field.copy(dataType = to)
      case field => field
    })
  }

  def field(name: String): Option[Field] = fields.find(_.name == name)

  def toLowerCase(): StructType = copy(fields = fields.map(_.toLowerCase()))

  def fieldNames(): List[String] = fields.map(_.name)

  def addField(name: String): StructType = addField(Field(name, StringType))

  def addField(field: Field): StructType = {
    require(!fieldNames().contains(field.name), s"Field ${field.name} already exists")
    copy(fields = fields :+ field)
  }

  def contains(fieldName: String, caseSensitive: Boolean = true): Boolean = {
    def contains(fields: Seq[Field]): Boolean = fields.exists { it =>
      (if (caseSensitive) fieldName == it.name else fieldName equalsIgnoreCase it.name) || fields
        .map(_.dataType).collect {
        case struct: StructType => struct.fields
      }.exists(contains)
    }
    contains(fields)
  }

  def stripFromFieldNames(chars: Seq[Char]): StructType = {
    def strip(name: String): String = chars.foldLeft(name) { (name, char) => name.replace(char.toString, "") }
    StructType(fields.map { field =>
      field.copy(name = strip(field.name))
    })
  }

  def addFieldIfNotExists(name: String): StructType = if (fieldNames().contains(name)) this else addField(Field(name, StringType))
  def addFieldIfNotExists(field: Field): StructType = if (fieldNames().contains(field.name)) this else addField(field)

  def updateFieldType(fieldName: String, dataType: DataType): StructType = copy(fields = fields.map { field =>
    if (field.name == fieldName) field.copy(dataType = dataType)
    else field
  })

  def removeFields(first: String, rest: String*): StructType = removeFields(first +: rest)
  def removeFields(names: Seq[String]): StructType = copy(fields = fields.filterNot { field =>
    names.contains(field.name)
  })

  def removeField(name: String, caseSensitive: Boolean = true): StructType = {
    copy(fields = fields.filterNot { field =>
      if (caseSensitive) field.name == name else field.name.equalsIgnoreCase(name)
    })
  }

  def size(): Int = fields.size

  def join(other: StructType): StructType = {
    require(
      fields.map(_.name).intersect(other.fields.map(_.name)).isEmpty,
      "Cannot join two structs which have common field names"
    )
    StructType(fields ++ other.fields)
  }

  def replaceField(sourceFieldName: String, targetField: Field): StructType = StructType(
    fields.map {
      case field if field.name == sourceFieldName => targetField
      case field => field
    }
  )

  def renameField(from: String, to: String): StructType = StructType(fields.map { field =>
    if (field.name == from) field.copy(name = to) else field
  })

  def show(): String = {
    "Struct\n" + fields.map { field =>
      val nullString = if (field.nullable) "nullable" else "not nullable"
      val partitionString = if (field.partition) "partition" else ""
      s"- ${field.name} [${field.dataType} $nullString $partitionString]"
    }.mkString("\n")
  }

  def ddl(table: String): String = {
    s"CREATE TABLE $table " + fields.map { it =>
      it.name + " " + it.dataType.toString.toLowerCase.stripSuffix("type")
    }.mkString("(", ", ", ")")
  }
}

object StructType {

  def fromFieldNames(names: Seq[String]): StructType = apply(names.map(Field.apply(_, StringType)))

  def apply(fields: Seq[Field]): StructType = apply(fields.toList)
  def apply(first: Field, rest: Field*): StructType = apply((first +: rest).toList)
  def apply(first: String, rest: String*): StructType = apply((first +: rest).map(name => Field(name, StringType)).toList)

  import scala.reflect.runtime.universe._

  def from[T <: Product : TypeTag]: StructType = {
    val fields = typeOf[T].decls.collect {
      case m: MethodSymbol if m.isCaseAccessor =>
        val javaClass = implicitly[TypeTag[T]].mirror.runtimeClass(m.returnType.typeSymbol.asClass)
        val dataType = SchemaFn.toFieldType(javaClass)
        Field(m.name.toString, dataType, true)
    }
    StructType(fields.toList)
  }
}

object SchemaFn {
  def toFieldType(clz: Class[_]): DataType = {
    val intClass = classOf[Int]
    val floatClass = classOf[Float]
    val stringClass = classOf[String]
    val charClass = classOf[Char]
    val bigIntClass = classOf[BigInt]
    val booleanClass = classOf[Boolean]
    val doubleClass = classOf[Double]
    val bigdecimalClass = classOf[BigDecimal]
    val longClass = classOf[Long]
    clz match {
      case `bigdecimalClass` => DecimalType(Scale(18), Precision(18))
      case `bigIntClass` => BigIntType
      case `booleanClass` => BooleanType
      case `doubleClass` => DoubleType
      case `intClass` => IntType(true)
      case `floatClass` => FloatType
      case `charClass` => CharType(1)
      case `longClass` => LongType(true)
      case `stringClass` => StringType
      case _ => sys.error(s"Can not map $clz to FieldType")
    }
  }
}