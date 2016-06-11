package io.eels.util

fun <T, U> Iterator<T>.map(f: (T) -> U): Iterator<U> {
  val outer = this
  return object : Iterator<U> {
    override fun hasNext(): Boolean = outer.hasNext()
    override fun next(): U = f(outer.next())
  }
}