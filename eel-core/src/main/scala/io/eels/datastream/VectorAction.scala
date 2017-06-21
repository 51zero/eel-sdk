package io.eels.datastream

import io.eels.Row

case class VectorAction(ds: DataStream) {
  def execute: Vector[Row] = ds.coalesce.toIterable.toVector
}
