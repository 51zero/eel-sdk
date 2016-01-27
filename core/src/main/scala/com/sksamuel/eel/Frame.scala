package com.sksamuel.eel

import com.sksamuel.eel.sink.{Field, Column, Row}

trait Frame {
  outer =>

  def schema: FrameSchema
  protected def iterator: Iterator[Row]

  def exists(p: (Row) => Boolean): Boolean = iterator.exists(p)
  def find(p: (Row) => Boolean): Option[Row] = iterator.find(p)
  def head: Option[Row] = iterator.take(1).toList.headOption

  def addColumn(name: String, value: String): Frame = new Frame {
    override val schema: FrameSchema = outer.schema.addColumn(name)
    override protected def iterator: Iterator[Row] = new Iterator[Row] {
      val iterator = outer.iterator
      override def hasNext: Boolean = iterator.hasNext
      override def next(): Row = iterator.next().addColumn(name, value)
    }
  }

  def removeColumn(name: String): Frame = new Frame {
    override protected def iterator: Iterator[Row] = new Iterator[Row] {
      val iterator = outer.iterator
      override def hasNext: Boolean = iterator.hasNext
      override def next(): Row = iterator.next().removeColumn(name)
    }
    override def schema: FrameSchema = outer.schema.removeColumn(name)
  }

  def union(frame: Frame): Frame = Frame(schema, () => outer.iterator ++ frame.iterator)

  def projection(first: String, rest: String*): Frame = projection(first +: rest)
  def projection(columns: Seq[String]): Frame = new Frame {
    override val schema: FrameSchema = FrameSchema(columns.map(Column.apply))
    override protected def iterator: Iterator[Row] = new Iterator[Row] {
      val iterator = outer.iterator
      val newColumns = columns.map(Column.apply)
      override def hasNext: Boolean = iterator.hasNext
      override def next(): Row = {
        val row = iterator.next()
        val map = row.columns.map(_.name).zip(row.fields.map(_.value)).toMap
        val fields = newColumns.map(col => Field(map(col.name)))
        Row(newColumns, fields)
      }
    }
  }

  def reduceLeft(f: (Row, Row) => Row): Frame = Frame(outer.iterator.reduceLeft(f))

  /**
    * Execute a side effect function for every row in the frame, returning the same Frame.
    *
    * @param f the function to execute
    * @return this frame, to allow for builder style chaining
    */
  def foreach[U](f: (Row) => U): Frame = new Frame {
    override protected def iterator: Iterator[Row] = new Iterator[Row] {
      val iterator = outer.iterator
      override def hasNext: Boolean = iterator.hasNext
      override def next: Row = {
        val row = iterator.next
        f(row)
        row
      }
    }
    override def schema: FrameSchema = outer.schema
  }

  def forall(p: (Row) => Boolean): Boolean = iterator.forall(p)

  def drop(k: Int): Frame = Frame(schema, () => outer.iterator.drop(k))

  def map(f: Row => Row): Frame = new Frame {
    override def schema: FrameSchema = outer.schema
    override def iterator: Iterator[Row] = new Iterator[Row] {
      val iterator = outer.iterator
      override def hasNext: Boolean = iterator.hasNext
      override def next: Row = f(iterator.next)
    }
  }

  def filterNot(p: Row => Boolean): Frame = filter(str => !p(str))
  def filter(p: Row => Boolean): Frame = Frame(schema, () => outer.iterator.filter(p))
  def filter(column: String, p: String => Boolean): Frame = Frame(
    schema,
    () => outer.iterator.filter(row => p(row(column)))
  )

  def size: Long = iterator.size

  def toList: List[Row] = iterator.toList

  def to(sink: Sink): Unit = {
    iterator.foreach { row =>
      sink.insert(row)
    }
    sink.completed()
  }
}

object Frame {

  private[eel] def apply(_schema: FrameSchema, iterfn: () => Iterator[Row]) = new Frame {
    override def schema: FrameSchema = _schema
    override protected def iterator: Iterator[Row] = iterfn()
  }

  def apply(first: Row, rest: Row*): Frame = new Frame {
    override def iterator: Iterator[Row] = (first +: rest).iterator
    override lazy val schema: FrameSchema = FrameSchema(first.columns)
  }

  def fromSource(source: Source): Frame = new Frame {
    override lazy val schema: FrameSchema = source.schema
    def iterator: Iterator[Row] = source.loader
  }
}