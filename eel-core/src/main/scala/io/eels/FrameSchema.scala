package io.eels

import scala.language.implicitConversions

case class FrameSchema(columns: List[Column]) {

  def indexOf(column: Column): Int = indexOf(column.name)
  def indexOf(columnName: String): Int = columns.indexWhere(_.name == columnName)

  def columnNames: List[String] = columns.map(_.name)

  def addColumn(col: Column): FrameSchema = copy(columns :+ col)

  def removeColumn(name: String): FrameSchema = copy(columns = columns.filterNot(_.name == name))
  def removeColumns(names: List[String]): FrameSchema = copy(columns = columns.filterNot(names contains _.name))

  def join(other: FrameSchema): FrameSchema = {
    require(
      columns.map(_.name).intersect(other.columns.map(_.name)).isEmpty,
      "Cannot join two frames which have duplicated column names"
    )
    FrameSchema(columns ++ other.columns)
  }

  def renameColumn(from: String, to: String): FrameSchema = FrameSchema(columns.map {
    case col@Column(`from`, _, _, _, _, _, _) => col.copy(name = to)
    case other => other
  })

  def print: String = {
    columns.map { column =>
      val str = s"- ${column.name} [${column.`type`}]"
      if (column.nullable) str + " (nullable)"
      else str
    }.mkString("\n")
  }
}

object FrameSchema {
  implicit def apply(strs: Seq[String]): FrameSchema = FrameSchema(strs.map(Column.apply).toList)
}