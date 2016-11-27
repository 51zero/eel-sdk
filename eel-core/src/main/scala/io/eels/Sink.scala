package io.eels

import com.sksamuel.exts.io.Using
import io.eels.schema.StructType

trait Sink extends Using {

  def write(rows: Seq[Row]): Unit = {
    require(rows.nonEmpty)
    using(writer(rows.head.schema)) { writer =>
      rows.foreach(writer.write)
    }
  }

  def writer(schema: StructType): SinkWriter
}

trait SinkWriter {
  def write(row: Row): Unit
  def close(): Unit
}