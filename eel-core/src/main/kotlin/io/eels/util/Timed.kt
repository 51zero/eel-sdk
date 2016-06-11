package io.eels.util

interface Timed {
  fun <T> timed(msg: String, fn: () -> T): T {
    val start = System.nanoTime()
    val t = fn()
    val end = System.nanoTime()
    val ms = (end - start) / 1000
    val secs = ms / 1000
    println("$msg took ${ms}ms ${secs}secs")
    return t
  }
}