package io.eels

/**
  * A Buffer represents a stream of Rows, read one row at a time. A Buffer is thread safe,
  * that is, multiple iterators can be created and accessed concurrently from the
  * same source buffer.
  */
interface Buffer {

  /**
    * Closes this buffer once it is no longer needed. This results in early termination of any
    * outstanding Iterators. That is, after this method is called, all further calls to `hasNext`
    * on the iterators will return false.
    */
  fun close(): Unit

  fun iterator(): Iterator<Array<Any>>
}