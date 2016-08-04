package io.eels.component

import io.eels.Row
import rx.Observable

/**
 * A Part represents part of the source data. Eg a single file in a multi-file source, or a single table
 * in a multi-table source. Splitting sources into parts allows them to be read concurrently, therefore,
 * implementations must ensure that different parts can be safely read in parallel.
 * A single part is always read by a single thread.
 */
interface Part {

  /**
   * Returns the data contained in this part in the form of an Observable that a subscriber can subscribe to.
   * This function should createReader a clean rows on each invocation. By clean, we mean that each
   * seperate rows should provide the full set of data contained in the part, in a thread safe manner.
   * Ie, it should be possible to invoke this method k times, and subscribe to those k observables concurrently,
   * and each rows should emit the same data.
   */
  fun data(): Observable<Row>
}