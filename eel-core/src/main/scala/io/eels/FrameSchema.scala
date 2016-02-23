package io.eels

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.component.hive.FrameSchemaFn

import scala.language.implicitConversions
import scala.reflect.ClassTag

case class FrameSchema(columns: List[Column]) {

  def apply(name: String): Column = columns.find(_.name == name).get

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
      val signedString = if (column.signed) "signed" else "unsigned"
      val nullString = if (column.nullable) "null" else "not null"
      s"- ${column.name} [${column.`type`} $nullString scale=${column.scale} precision=${column.precision} $signedString]"
    }.mkString("\n")
  }
}

object FrameSchema {
  def apply(first: String, rest: String*): FrameSchema = apply(first +: rest)

  implicit def apply(strs: Seq[String]): FrameSchema = FrameSchema(strs.map(Column.apply).toList)

  import scala.reflect.runtime.universe._

  def from[T <: Product]()(implicit classTag: ClassTag[T], ttag: TypeTag[T]): FrameSchema = {

    val columns = typeOf[T].decls.collect { case m: MethodSymbol if m.isCaseAccessor =>
      val (schemaType, _, _) = FrameSchemaFn.toSchemaType(m.returnType.toString)
      Column(m.name.toString, schemaType, true)
    }

    FrameSchema(columns.toList)
  }
}
