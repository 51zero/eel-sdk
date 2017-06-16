package io.eels

import java.io.Closeable

import scala.collection.{GenTraversableOnce, Iterator}

case class CloseIterator[T](closeable: Closeable, iterator: Iterator[T]) extends Closeable with Iterator[T] {
  override def close(): Unit = closeable.close()
  override def hasNext: Boolean = iterator.hasNext
  override def next(): T = iterator.next
  override def map[B](f: T => B): CloseIterator[B] = CloseIterator[B](closeable, iterator.map(f))
  override def filter(p: T => Boolean): CloseIterator[T] = CloseIterator[T](closeable, iterator.filter(p))
  override def drop(n: Int): CloseIterator[T] = CloseIterator[T](closeable, iterator.drop(n))
  override def take(n: Int): CloseIterator[T] = CloseIterator[T](closeable, iterator.take(n))
  override def dropWhile(p: T => Boolean): CloseIterator[T] = CloseIterator[T](closeable, iterator.dropWhile(p))
  override def flatMap[B](f: T => GenTraversableOnce[B]): CloseIterator[B] = CloseIterator[B](closeable, iterator.flatMap(f))
  override def takeWhile(p: T => Boolean): CloseIterator[T] = CloseIterator[T](closeable, iterator.takeWhile(p))
  def merge(other: Iterator[T]): CloseIterator[T] = CloseIterator[T](closeable, iterator ++ other)
}

object CloseIterator {
  def empty: CloseIterator[Row] = CloseIterator(new Closeable {
    override def close(): Unit = ()
  }, Iterator.empty)
}
