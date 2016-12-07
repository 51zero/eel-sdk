package io.eels

import scala.collection.Iterator._

trait CloseableIterator[+T] {
  self =>

  def hasNext(): Boolean
  def next(): T
  def close(): Unit

  def foreach[U](f: T => U) {
    while (hasNext) f(next())
  }

  def map[B](f: T => B): CloseableIterator[B] = new CloseableIterator[B] {
    def hasNext() = self.hasNext
    def next() = f(self.next())
    override def close(): Unit = self.close()
  }

  def takeWhile(p: T => Boolean): CloseableIterator[T] = new CloseableIterator[T] {

    private var hd: T = _
    private var hdDefined: Boolean = false
    private var tail: CloseableIterator[T] = self

    override def close(): Unit = self.close()

    override def hasNext() = hdDefined || tail.hasNext && {
      hd = tail.next()
      if (p(hd)) hdDefined = true
      else tail = CloseableIterator.empty
      hdDefined
    }

    override def next() = if (hasNext) {
      hdDefined = false
      hd
    } else empty.next()
  }
}

object CloseableIterator {

  def fromIterable[T](rows: Seq[T]): CloseableIterator[T] = new CloseableIterator[T] {
    private val iter = rows.iterator
    private var closed = false
    override def hasNext(): Boolean = !closed && iter.hasNext
    override def next(): T = iter.next
    override def close(): Unit = closed = true
  }

  val empty: CloseableIterator[Nothing] = new CloseableIterator[Nothing] {
    override def hasNext(): Boolean = false
    override def next(): Nothing = throw new UnsupportedOperationException()
    override def close(): Unit = ()
  }
}
