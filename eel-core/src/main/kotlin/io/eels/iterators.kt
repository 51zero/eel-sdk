package io.eels

fun <T, U> Iterator<T>.map(f: (T) -> U): Iterator<U> {
  val outer = this
  return object : Iterator<U> {
    override fun hasNext(): Boolean = this.hasNext()
    override fun next(): U = f(outer.next())
  }
}