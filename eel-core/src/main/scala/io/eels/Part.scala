package io.eels

import io.reactivex.Flowable

/**
 * A Part represents part of the source data. Eg a single file in a multi-file source, or a single table
 * in a multi-table source. Splitting sources into parts allows them to be read concurrently, therefore,
 * implementations must ensure that different parts can be safely read in parallel.
 * A single part is always read by a single thread.
  */
trait Part {

  /**
    * Returns the data contained in this part in the form of a Flowable. This function should
    * return a new Flowable on each invocation.
    */
  def open(): Flowable[Row]
}