package io.eels

trait Part {
  outer =>

  def iterator: Iterator[Row]

  def filter(column: String, p: (String) => Boolean): Part = new Part {
    override def iterator: Iterator[Row] = outer.iterator.filter(row => p(row(column)))
  }

  def filter(p: (Row) => Boolean): Part = new Part {
    override def iterator: Iterator[Row] = outer.iterator.filter(p)
  }

  def map(f: (Row) => Row): Part = new Part {
    override def iterator: Iterator[Row] = outer.iterator.map(f)
  }

  def collect(pf: PartialFunction[Row, Row]): Part = new Part {
    override def iterator: Iterator[Row] = outer.iterator.collect(pf)
  }

  def foreach[U](f: (Row) => U): Part = new Part {
    override def iterator: Iterator[Row] = new Iterator[Row] {
      val iter = outer.iterator
      override def hasNext: Boolean = iter.hasNext
      override def next: Row = {
        val row = iter.next
        f(row)
        row
      }
    }
  }

  def projection(columns: Seq[String]): Part = new Part {
    override def iterator: Iterator[Row] = new Iterator[Row] {
      val iter = outer.iterator
      val newColumns = columns.map(Column.apply)
      override def hasNext: Boolean = iter.hasNext
      override def next(): Row = {
        val row = iter.next()
        val map = row.columns.map(_.name).zip(row.fields.map(_.value)).toMap
        val fields = newColumns.map(col => Field(map(col.name)))
        Row(newColumns.toList, fields.toList)
      }
    }
  }

  def addColumn(name: String, defaultValue: String): Part = new Part {
    override def iterator: Iterator[Row] = new Iterator[Row] {
      val iter = outer.iterator
      override def hasNext: Boolean = iter.hasNext
      override def next(): Row = iter.next().addColumn(name, defaultValue)
    }
  }

  def removeColumn(name: String): Part = Part(
    () => new Iterator[Row] {
      val iter = outer.iterator
      override def hasNext: Boolean = iter.hasNext
      override def next(): Row = iter.next().removeColumn(name)
    }
  )
}

object Part {
  def apply(f: () => Iterator[Row]): Part = new Part {
    override def iterator: Iterator[Row] = f()
  }
}