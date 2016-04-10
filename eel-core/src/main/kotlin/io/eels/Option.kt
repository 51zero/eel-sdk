package io.eels

interface Option<out T> {
  fun <U> map(f: (T) -> U): Option<U>
  fun <U> fold(emptyValue: U, f: (T) -> U): U
}

object None : Option<Nothing> {
  override fun <U> fold(emptyValue: U, f: (Nothing) -> U): U = emptyValue
  override fun <U> map(f: (Nothing) -> U): Option<U> = None
}

class Some<T>(val value: T) : Option<T> {
  override fun <U> fold(emptyValue: U, f: (T) -> U): U = f(value)
  override fun <U> map(f: (T) -> U): Option<U> = Some(f(value))
}
