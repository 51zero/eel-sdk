package io.eels.component.parquet

import org.apache.parquet.hadoop.ParquetReader

/**
  * Creates an Iterator[T] for the data contained in a parquet reader.
  */
object ParquetIterator {
  def apply[T](reader: ParquetReader[T]): Iterator[T] = {
    Iterator.continually(reader.read).takeWhile(_ != null)
  }
}