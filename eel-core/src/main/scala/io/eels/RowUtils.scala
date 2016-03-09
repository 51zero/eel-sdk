package io.eels

object RowUtils {

  def replaceByFn(index: Int, fn: Any => Any, row: InternalRow): InternalRow = {
    (row.slice(0, index) :+ fn(row(index))) ++ row.slice(index + 1, row.length)
  }

  def replace(from: String, target: Any, row: InternalRow): InternalRow = row.map {
    case `from` => target
    case other => other
  }

  def replace(index: Int, from: String, target: Any, row: InternalRow): InternalRow = {
    (row.slice(0, index) :+ (row(index) match {
      case `from` => target
      case other => other
    })) ++ row.slice(index + 1, row.length)
  }

  def toMap(schema: Schema, row: InternalRow): Map[String, Any] = {
    require(schema.size == row.size)
    schema.columnNames.zip(row).map { case (field, value) => field -> value }.toMap
  }
}
