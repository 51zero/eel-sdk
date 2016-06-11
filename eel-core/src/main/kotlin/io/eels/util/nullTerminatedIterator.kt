package io.eels.util

import java.util.*
import java.util.stream.Stream

fun <T> Stream<T>.nullTerminatedIterator(): Iterator<T> = nullTerminatedIterator(this.iterator())

fun <T> nullTerminatedIterator(iter: Iterator<T>): Iterator<T> = object : Iterator<T> {

  var head: T? = null
  var headDefined = false

  override fun hasNext(): Boolean {
    if (headDefined) return head != null
    if (!iter.hasNext()) return false
    head = iter.next()
    headDefined = true
    return head != null
  }

  override fun next() = if (hasNext()) {
    headDefined = false; head!!
  } else throw NoSuchElementException("next on end of iterator")
}