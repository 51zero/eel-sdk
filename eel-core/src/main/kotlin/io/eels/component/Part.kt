package io.eels.component

import io.eels.Row

/**
 *
 * A Part represents part of the source data. Eg a single path in a multifile source, or a single table
 * in a multitable source. A part provides a reader for that source when requested.
 */
interface Part {
  fun reader(): SourceReader
}

interface SourceReader {
  fun close(): Unit
  fun iterator(): Iterator<Row>
}