package com.sksamuel.hs

import com.sksamuel.hs.sink.Row

trait Frame {
  outer =>

  protected def iterator: Iterator[Row]

  def drop(k: Int): Frame = new Frame {
    override def iterator: Iterator[Row] = outer.iterator.drop(k)
  }

  def map(f: Row => Row): Frame = new Frame {
    override def iterator: Iterator[Row] = new Iterator[Row] {
      val iterator = outer.iterator
      override def hasNext: Boolean = iterator.hasNext
      override def next: Row = f(iterator.next)
    }
  }

  def filterNot(p: Row => Boolean): Frame = filter(str => !p(str))
  def filter(p: Row => Boolean): Frame = new Frame {
    override def iterator: Iterator[Row] = new Iterator[Row] {
      val iterator = outer.iterator.filter(p)
      override def hasNext: Boolean = iterator.hasNext
      override def next: Row = iterator.next
    }
  }

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

  def apply(rows: Row*): Frame = new Frame {
    override def iterator: Iterator[Row] = rows.iterator
  }

  def fromSource(source: Source): Frame = new Frame {
    def iterator: Iterator[Row] = source.loader
  }
}