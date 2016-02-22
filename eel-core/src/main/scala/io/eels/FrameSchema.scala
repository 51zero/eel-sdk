package io.eels

import scala.language.implicitConversions

case class FrameSchema(columns: List[Column]) {
  require(columns.map(_.name).distinct.size == columns.size, "Frame schema cannot have duplicated column names")

  def apply(name: String): Column = columns.find(_.name == name).get

  def indexOf(column: Column): Int = indexOf(column.name)
  def indexOf(columnName: String): Int = columns.indexWhere(_.name == columnName)

  def columnNames: List[String] = columns.map(_.name)

  def addColumn(col: Column): FrameSchema = {
    require(!columnNames.contains(col.name), s"Column ${col.name} already exists")
    copy(columns :+ col)
  }

  def removeColumn(name: String): FrameSchema = copy(columns = columns.filterNot(_.name == name))
  def removeColumns(names: List[String]): FrameSchema = copy(columns = columns.filterNot(names contains _.name))

  def join(other: FrameSchema): FrameSchema = {
    require(
      columns.map(_.name).intersect(other.columns.map(_.name)).isEmpty,
      "Cannot join two frames which have duplicated column names"
    )
    FrameSchema(columns ++ other.columns)
  }

  def updateColumn(column: Column): FrameSchema = {
    val cols = columns.map {
      case col if col.name == column.name => column
      case col => col
    }
    FrameSchema(cols)
  }

  def renameColumn(from: String, to: String): FrameSchema = FrameSchema(columns.map {
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
}

object FrameSchema {
  def apply(first: Column, rest: Column*): FrameSchema = apply((first +: rest).toList)
  def apply(first: String, rest: String*): FrameSchema = apply(first +: rest)
  implicit def apply(strs: Seq[String]): FrameSchema = FrameSchema(strs.map(Column.apply).toList)
}