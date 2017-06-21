package io.eels

import java.io.Closeable

import scala.collection.{GenTraversableOnce, Iterator}

case class Channel[T](closeable: Closeable, iterator: Iterator[T]) extends Closeable with Iterator[T] {
  override def close(): Unit = closeable.close()
  override def hasNext: Boolean = iterator.hasNext
  override def next(): T = iterator.next
  override def foreach[U](f: T => U): Unit = iterator.foreach(f)
  override def map[B](f: T => B): Channel[B] = Channel[B](closeable, iterator.map(f))
  override def filter(p: T => Boolean): Channel[T] = Channel[T](closeable, iterator.filter(p))
  override def filterNot(p: T => Boolean): Channel[T] = filter(!p(_))
  override def drop(n: Int): Channel[T] = Channel[T](closeable, iterator.drop(n))
  override def take(n: Int): Channel[T] = Channel[T](closeable, iterator.take(n))
  override def dropWhile(p: T => Boolean): Channel[T] = Channel[T](closeable, iterator.dropWhile(p))
  override def flatMap[B](f: T => GenTraversableOnce[B]): Channel[B] = Channel[B](closeable, iterator.flatMap(f))
  override def takeWhile(p: T => Boolean): Channel[T] = Channel[T](closeable, iterator.takeWhile(p))
  def merge(other: Iterator[T]): Channel[T] = Channel[T](closeable, iterator ++ other)
}

object Channel {

  def apply[T](iterator: Iterator[T]): Channel[T] = Channel(new Closeable {
    override def close(): Unit = ()
  }, iterator)

  def apply[T](closefn: () => Unit, iterator: Iterator[T]): Channel[T] = Channel(new Closeable {
    override def close(): Unit = closefn()
  }, iterator)

  def empty[T]: Channel[T] = Channel(new Closeable {
    override def close(): Unit = ()
  }, Iterator.empty)
}
