package io.eels

import com.sksamuel.exts.io.Using
import io.eels.schema.StructType

trait Sink extends Using {

  // opens up n writers. This allows the sink to optimize the cases when it knows it will be
  // writing multiple files. For example, in hive, it can create separate output streams that can
  // safely write into the same partitions.
  def open(schema: StructType, n: Int): Seq[SinkWriter] = List.fill(n) {
    open(schema)
  }

  def open(schema: StructType): SinkWriter
}

/**
  * A RowWriter writes `Row`s to some storage area.
  *
  * It does not need to be thread safe, callers must guarantee that only a single thread
  * will invoke a particular writer at a time.
  *
  * `RowWriter`s can be implemented as lazy if required, so that file handles, etc, are not
  * opened until the first record is written.
  */
trait SinkWriter {

  def write(row: Rec): Unit

  // closes this writer and performs any other "completion" actions
  // if multiple writers have been opened, then close should be called on them all at the same time
  // for instance, in case they are committing files from a staging area to a public area
  def close(): Unit
}