package io.eels

import scala.language.implicitConversions
import scala.reflect.ClassTag

case class Schema(columns: List[Column]) {

  require(columns.map(_.name).distinct.size == columns.size, "Frame schema cannot have duplicated column names")

  def apply(name: String): Column = columns.find(_.name == name).get

  def indexOf(column: Column): Int = indexOf(column.name, true)
  def indexOf(column: Column, caseSensitive: Boolean): Int = indexOf(column.name, caseSensitive)

  def indexOf(columnName: String): Int = indexOf(columnName, true)
  def indexOf(columnName: String, caseSensitive: Boolean): Int = columns.indexWhere { column =>
    if (caseSensitive) columnName == column.name else columnName equalsIgnoreCase column.name
  }

  lazy val columnNames: List[String] = columns.map(_.name)

  def addColumn(col: Column): Schema = {
    require(!columnNames.contains(col.name), s"Column ${col.name} already exists")
    copy(columns :+ col)
  }

  def stripFromColumnName(chars: Seq[Char]): Schema = {
    def strip(name: String): String = chars.foldLeft(name) { (str, char) => str.replace(char.toString, "") }
    Schema(columns.map(col => col.copy(name = strip(col.name))))
  }

  def addColumnIfNotExists(col: Column): Schema = if (columnNames.contains(col.name)) this else addColumn(col)

  def updateSchemaType(columnName: String, schemaType: SchemaType): Schema = {
    Schema(columns.map {
      case col@Column(`columnName`, _, _, _, _, _, _) => col.copy(`type` = schemaType)
      case col => col
    })
  }

  def removeColumn(name: String, caseSensitive: Boolean = true): Schema = {
    copy(columns = columns.filterNot { column =>
      if (caseSensitive) column.name == name else column.name equalsIgnoreCase name
    })
  }

  def size: Int = columns.size

  def removeColumns(names: List[String]): Schema = copy(columns = columns.filterNot(names contains _.name))

  def join(other: Schema): Schema = {
    require(
      columns.map(_.name).intersect(other.columns.map(_.name)).isEmpty,
      "Cannot join two frames which have duplicated column names"
    )
    Schema(columns ++ other.columns)
  }

  def updateColumn(column: Column): Schema = {
    val cols = columns.map {
      case col if col.name == column.name => column
      case col => col
    }
    Schema(cols)
  }

  def renameColumn(from: String, to: String): Schema = Schema(columns.map {
    case col@Column(`from`, _, _, _, _, _, _) => col.copy(name = to)
    case other => other
  })

  def print: String = {
    columns.map { column =>
      val signedString = if (column.signed) "signed" else "unsigned"
      val nullString = if (column.nullable) "null" else "not null"
      s"- ${column.name} [${column.`type`} $nullString scale=${column.scale} precision=${column.precision} $signedString]"
    }.mkString("\n")
  }

  def ddl(table: String): String = {
    s"CREATE TABLE $table " + columns.map(col => col.name + " " + col.`type`).mkString("(", ", ", ")")
  }
}

object Schema {
  def apply(first: Column, rest: Column*): Schema = apply((first +: rest).toList)
  def apply(first: String, rest: String*): Schema = apply(first +: rest)

  implicit def apply(strs: Seq[String]): Schema = Schema(strs.map(Column.apply).toList)

  import scala.reflect.runtime.universe._

  def from[T <: Product : TypeTag : ClassTag]: Schema = {
    val columns = typeOf[T].declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor =>
        val javaClass = implicitly[TypeTag[T]].mirror.runtimeClass(m.returnType.typeSymbol.asClass)
        val schemaType = SchemaFn.toSchemaType(javaClass)
        Column(m.name.toString, schemaType, true)
    }
    Schema(columns.toList)
  }
}

object SchemaFn {
  def toSchemaType(clz: Class[_]): SchemaType = {
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
      case `intClass` => SchemaType.Int
      case `floatClass` => SchemaType.Float
      case `stringClass` => SchemaType.String
      case `charClass` => SchemaType.String
      case `bigIntClass` => SchemaType.BigInt
      case `booleanClass` => SchemaType.Boolean
      case `doubleClass` => SchemaType.Double
      case `longClass` => SchemaType.Long
      case `bigdecimalClass` => SchemaType.Decimal
      case _ => sys.error(s"Can not map $clz to SchemaType value.")
    }
  }
}
