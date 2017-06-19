package io.eels

import com.sksamuel.exts.io.Using
import io.eels.schema.StructType

trait Sink extends Using {

  def write(rows: Seq[Row]): Unit = {
    require(rows.nonEmpty)
    using(open(rows.head.schema)) { writer =>
      rows.foreach(writer.write)
    }
  }

  // opens up n output streams. This allows the sink to optimize the cases when it knows it will be
  // writing multiple files. For example, in hive, it can create separate output streams that can
  // safely write into the same partitions.
  def open(schema: StructType, n: Int): Seq[RowOutputStream] = List.fill(n) {
    open(schema)
  }

  def open(schema: StructType): RowOutputStream
}

/**
  * A RowOutputStream writes Row data to some storage.
  *
  * It does not need to be thread safe, callers will guarantee that only a single thread
  * will invoke a particular output stream at a time.
  *
  * RowOutputStream's can be implemented as lazy if required, so that file handles, etc, are not
  * opened until the first record is written.
  */
trait RowOutputStream {
  def write(row: Row): Unit
  def close(): Unit
}