package io.eels

trait CloseableIterator[+T] {
  self =>

  // the real iterator
  val iterator: Iterator[T]

  val iter = new Iterator[T] {
    override def hasNext: Boolean = !isClosed() && iterator.hasNext
    override def next(): T = iterator.next()
  }

  private var closed = false
  final def isClosed(): Boolean = closed
  def close(): Unit = closed = true

  def foreach[U](f: T => U) = iter.foreach(f)

  def head: T = {
    val h = iterator.take(1).toList.head
    close()
    h
  }

  def map[U](f: T => U): CloseableIterator[U] = new CloseableIterator[U] {
    override def close(): Unit = self.close()
    override val iterator: Iterator[U] = self.iter.map(f)
  }

  def foldLeft[U](z: U)(op: (U, T) => U) = iterator.foldLeft(z)(op)

  def dropWhile(p: T => Boolean): CloseableIterator[T] = new CloseableIterator[T] {
    override def close(): Unit = self.close()
    override val iterator: Iterator[T] = self.iter.dropWhile(p)
  }

  def drop(k: Int): CloseableIterator[T] = new CloseableIterator[T] {
    override def close(): Unit = self.close()
    override val iterator: Iterator[T] = self.iter.drop(k)
  }

  def size = iter.size

  def take(k: Int): CloseableIterator[T] = new CloseableIterator[T] {
    override def close(): Unit = self.close()
    override val iterator: Iterator[T] = self.iter.take(k)
  }

  def takeWhile(p: T => Boolean): CloseableIterator[T] = new CloseableIterator[T] {
    override def close(): Unit = self.close()
    override val iterator: Iterator[T] = self.iter.takeWhile(p)
  }

  def flatMap[U](fn: T => Iterable[U]) = new CloseableIterator[U] {
    override def close(): Unit = self.close()
    override val iterator: Iterator[U] = self.iter.flatMap(fn)
  }

  def filter(p: T => Boolean) = new CloseableIterator[T] {
    override def close(): Unit = self.close()
    override val iterator: Iterator[T] = self.iter.filter(p)
  }

  def concat[U >: T](other: CloseableIterator[U]) = new CloseableIterator[U] {

    override def close(): Unit = {
      self.close()
      other.close()
    }

    override val iterator: Iterator[U] = self.iter ++ other.iter
  }

  def toList: List[T] = iter.toList
  def toVector: Vector[T] = iter.toVector

  def forall(p: T => Boolean) = iter.forall(p)
  def exists(p: T => Boolean) = iter.exists(p)
  def find(p: T => Boolean) = iter.find(p)

  def zip[U](other: CloseableIterator[U]) = new CloseableIterator[(T, U)] {

    override def close(): Unit = {
      self.close()
      other.close()
    }

    override val iterator: Iterator[(T, U)] = self.iter.zip(other.iter)
  }
}

object CloseableIterator {

  def fromIterable[T](rows: Seq[T]): CloseableIterator[T] = new CloseableIterator[T] {
    override val iterator: Iterator[T] = rows.iterator
  }

  val empty: CloseableIterator[Nothing] = new CloseableIterator[Nothing] {
    override val iterator: Iterator[Nothing] = Iterator.empty
  }
}
