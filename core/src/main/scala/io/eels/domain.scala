package io.eels

import scala.language.implicitConversions

object Field {
  implicit def toField(str: String): Field = Field(str)
}

case class Field(value: String)

case class Column(name: String,
                  `type`: SchemaType,
                  nullable: Boolean,
                  precision: Int = 0,
                  scale: Int = 0,
                  signed: Boolean = true,
                  comment: Option[String] = None)

sealed trait SchemaType
object SchemaType {
  case object BigInt extends SchemaType
  case object Binary extends SchemaType
  case object Boolean extends SchemaType
  case object Date extends SchemaType
  case object Decimal extends SchemaType
  case object Double extends SchemaType
  case object Float extends SchemaType
  case object Int extends SchemaType
  case object Long extends SchemaType
  case object Short extends SchemaType
  case object String extends SchemaType
  case object Timestamp extends SchemaType
  case object Unsupported extends SchemaType
}

object Column {
  implicit def apply(str: String): Column = Column(str, SchemaType.String, false)
}

object Row {
  val Sentinel: Row = Row(Nil, Nil)

  def apply(map: Map[String, String]): Row = {
    Row(map.keys.map(Column.apply).toList, map.values.seq.map(Field.apply).toList)
  }
}

case class Row(columns: List[Column], fields: List[Field]) {

  require(columns.size == fields.size, "Columns and fields should have the same size")

  def apply(name: String): String = {
    val pos = columns.indexWhere(_.name == name)
    fields(pos).value
  }

  def join(other: Row): Row = Row(columns ++ other.columns, fields ++ other.fields)

  def size: Int = columns.size

  def addColumn(name: String, value: String): Row = {
    copy(columns = columns :+ Column(name), fields = fields :+ Field(value))
  }

  def removeColumn(name: String): Row = Row(toMap - name)

  def toMap: Map[String, String] = columns.map(_.name).zip(fields.map(_.value)).toMap
}
