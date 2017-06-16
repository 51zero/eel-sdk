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
  // writing multiple files. For example, in hive, it can create seperate files safely.
  def open(schema: StructType, n: Int): Seq[RowOutputStream] = List.fill(n) {
    open(schema)
  }

  def open(schema: StructType): RowOutputStream
}

// output streams may be lazy until the first record is written.
trait RowOutputStream {
  def write(row: Row): Unit
  def close(): Unit
}