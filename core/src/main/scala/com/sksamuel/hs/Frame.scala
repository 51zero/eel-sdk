package com.sksamuel.hs

trait Frame {
  outer =>

  def hasNext: Boolean
  def next: Seq[String]

  def map(f: String => String): Frame = new Frame {
    override def hasNext: Boolean = outer.hasNext
    override def next: Seq[String] = outer.next.map(f)
  }

  def filter(p: String => Boolean): Frame = new Frame {
    override def hasNext: Boolean = outer.hasNext
    override def next: Seq[String] = outer.next.filter(p)
  }

  def size: Long = toIterator.size

  private def toIterator: Iterator[Seq[String]] = new Iterator[Seq[String]] {
    override def hasNext: Boolean = outer.hasNext
    override def next(): Seq[String] = outer.next
  }
}

object Frame {
  def fromSource(source: Source): Frame = new Frame {
    val load = source.loader
    override def hasNext: Boolean = load.hasNext
    override def next: Seq[String] = load.next()
  }
}