package io.eels

import java.io.Closeable

import scala.collection.{GenTraversableOnce, Iterator}

case class CloseableIterator[T](closeable: Closeable, iterator: Iterator[T]) extends Closeable with Iterator[T] {
  override def close(): Unit = closeable.close()
  override def hasNext: Boolean = iterator.hasNext
  override def next(): T = iterator.next
  override def foreach[U](f: T => U): Unit = iterator.foreach(f)
  override def map[B](f: T => B): CloseableIterator[B] = CloseableIterator[B](closeable, iterator.map(f))
  override def filter(p: T => Boolean): CloseableIterator[T] = CloseableIterator[T](closeable, iterator.filter(p))
  override def filterNot(p: T => Boolean): CloseableIterator[T] = filter(!p(_))
  override def drop(n: Int): CloseableIterator[T] = CloseableIterator[T](closeable, iterator.drop(n))
  override def take(n: Int): CloseableIterator[T] = CloseableIterator[T](closeable, iterator.take(n))
  override def dropWhile(p: T => Boolean): CloseableIterator[T] = CloseableIterator[T](closeable, iterator.dropWhile(p))
  override def flatMap[B](f: T => GenTraversableOnce[B]): CloseableIterator[B] = CloseableIterator[B](closeable, iterator.flatMap(f))
  override def takeWhile(p: T => Boolean): CloseableIterator[T] = CloseableIterator[T](closeable, iterator.takeWhile(p))
  def merge(other: Iterator[T]): CloseableIterator[T] = CloseableIterator[T](closeable, iterator ++ other)
}

object CloseableIterator {

  def apply[T](iterator: Iterator[T]): CloseableIterator[T] = CloseableIterator(new Closeable {
    override def close(): Unit = ()
  }, iterator)

  def apply[T](closefn: () => Unit, iterator: Iterator[T]): CloseableIterator[T] = CloseableIterator(new Closeable {
    override def close(): Unit = closefn()
  }, iterator)

  def empty[T]: CloseableIterator[T] = CloseableIterator(new Closeable {
    override def close(): Unit = ()
  }, Iterator.empty)
}
