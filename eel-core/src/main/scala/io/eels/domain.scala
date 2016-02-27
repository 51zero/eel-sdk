package io.eels

import scala.language.implicitConversions

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

object InternalRow {
  val PoisonPill: InternalRow = List(new {})
}

case class Row(schema: Schema, values: Seq[Any]) {
  override def toString(): String = {
    schema.columnNames.zip(values).map { case (column, value) => s"$column = ${if (value == null) "" else value.toString}" }.mkString("[", ",", "]")
  }
}

object Row {
  def apply(schema: Schema, first: Any, rest: Any*): Row = Row(schema, (first +: rest).toIndexedSeq)
}