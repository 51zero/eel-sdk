package com.sksamuel.eel

import scala.language.implicitConversions

object Field {
  implicit def toField(str: String): Field = Field(str)
}

case class Field(value: String)

case class Column(name: String)

object Column {
  implicit def toField(str: String): Column = Column(str)
}

object Row {
  def apply(map: Map[String, String]): Row = {
    Row(map.keys.map(Column.apply).toSeq, map.values.seq.map(Field.apply).toSeq)
  }
}

case class Row(columns: Seq[Column], fields: Seq[Field]) {
  require(columns.size == fields.size, "Columns and fields should have the same size")

  def apply(name: String): String = {
    val pos = columns.indexWhere(_.name == name)
    fields(pos).value
  }

  def size: Int = columns.size

  def addColumn(name: String, value: String): Row = {
    copy(columns = columns :+ Column(name), fields = fields :+ Field(value))
  }

  def removeColumn(name: String): Row = Row(toMap - name)

  def toMap: Map[String, String] = columns.map(_.name).zip(fields.map(_.value)).toMap
}
