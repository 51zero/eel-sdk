package io.eels.component

interface Using {
  fun <T : AutoCloseable, U> using(resource: T, fn: (T) -> U): U {
    return try {
      fn(resource)
    } finally {
      resource.close()
    }
  }
}