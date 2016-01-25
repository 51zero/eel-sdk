package com.sksamuel.hs

import com.sksamuel.hs.sink.Sink

trait Frame {
  outer =>

  def to(sink: Sink): Unit = {
    toIterator.foreach { row =>
      sink.insert(row)
    }
    sink.completed()
  }

  def hasNext: Boolean
  def next: Seq[String]

  def drop(k: Int): Frame = new Frame {
    var j = 0
    override def hasNext: Boolean = {
      while (j < k && outer.hasNext) {
        outer.next
        j += 1
      }
      outer.hasNext
    }
    override def next: Seq[String] = outer.next
  }

  def map(f: String => String): Frame = new Frame {
    override def hasNext: Boolean = outer.hasNext
    override def next: Seq[String] = outer.next.map(f)
  }

  def filter(p: String => Boolean): Frame = new Frame {
    override def hasNext: Boolean = outer.hasNext
    override def next: Seq[String] = outer.next.filter(p)
  }

  def filterNot(p: String => Boolean): Frame = filter(str => !p(str))

  def size: Long = toIterator.size

  private def toIterator: Iterator[Seq[String]] = new Iterator[Seq[String]] {
    override def hasNext: Boolean = outer.hasNext
    override def next(): Seq[String] = outer.next
  }
}

object Frame {

  def fromSeq(seq: Seq[Seq[String]]): Frame = new Frame {
    val iterator = seq.iterator
    override def hasNext: Boolean = iterator.hasNext
    override def next: Seq[String] = iterator.next()
  }

  def fromSource(source: Source): Frame = new Frame {
    val load = source.loader
    override def hasNext: Boolean = load.hasNext
    override def next: Seq[String] = load.next()
  }
}