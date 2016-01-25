package com.sksamuel.hs

import com.sksamuel.hs.sink.Row

trait Frame {
  outer =>

  def hasNext: Boolean
  def next: Row

  def drop(k: Int): Frame = new Frame {
    var j = 0
    override def hasNext: Boolean = {
      while (j < k && outer.hasNext) {
        outer.next
        j += 1
      }
      outer.hasNext
    }
    override def next: Row = outer.next
  }

  def map(f: Row => Row): Frame = new Frame {
    override def hasNext: Boolean = outer.hasNext
    override def next: Row = f(outer.next)
  }

  def filterNot(p: Row => Boolean): Frame = filter(str => !p(str))
  def filter(p: Row => Boolean): Frame = new Frame {
    override def hasNext: Boolean = outer.hasNext
    override def next: Row = outer.next
  }

  def size: Long = toIterator.size

  def to(sink: Sink): Unit = {
    toIterator.foreach { row =>
      sink.insert(row)
    }
    sink.completed()
  }

  private def toIterator: Iterator[Row] = new Iterator[Row] {
    override def hasNext: Boolean = outer.hasNext
    override def next(): Row = outer.next
  }
}

object Frame {

  def apply(rows: Row*): Frame = new Frame {
    val iterator = rows.iterator
    override def hasNext: Boolean = iterator.hasNext
    override def next: Row = iterator.next()
  }

  def fromSource(source: Source): Frame = new Frame {
    val load = source.loader
    override def hasNext: Boolean = load.hasNext
    override def next: Row = load.next()
  }
}