package io.eels.util

fun <T> List<T>.zipWithIndex(): List<Pair<T, Int>> = this.zip(0..this.size)

fun <T> List<T>.findOptional(predicate: (T) -> Boolean): Option<T> = Option(this.find { predicate(it) })