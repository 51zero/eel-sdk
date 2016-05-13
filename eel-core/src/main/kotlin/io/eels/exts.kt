package io.eels

fun <T> List<T>.zipWithIndex(): List<Pair<T, Int>> = this.zip(0..this.size)
