package io.eels

import java.util.stream.Stream

fun <T> Stream<T>.nullTerminatedIterator(): Iterator<T> = nullTerminatedIterator(this.iterator())

fun <T> nullTerminatedIterator(iter: Iterator<T>): Iterator<T> = object : Iterator<T> {

  var head: T? = null
  var defined = false

  override fun next(): T {
    return if (defined) head!!
    else iter.next()
  }

  override fun hasNext(): Boolean {
    return if (defined) head != null
    else iter.hasNext()
  }
}