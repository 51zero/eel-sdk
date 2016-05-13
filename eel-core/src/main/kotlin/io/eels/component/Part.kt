package io.eels.component

import io.eels.Row
import rx.Observable

/**
 * A Part represents part of the source data. Eg a single file in a multi-file source, or a single table
 * in a multi-table source. Splitting sources into parts allows them to be read concurrently. A single
 * part is always read by a single thread.
 */
interface Part {
  fun stream(): Observable<Row>
}