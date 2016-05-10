package io.eels.component

import java.io.Closeable

interface Using {
  fun <T : Closeable, U> using(resource: T, fn: (T) -> U): U {
    return try {
      fn(resource)
    } finally {
      resource.close()
    }
  }
}