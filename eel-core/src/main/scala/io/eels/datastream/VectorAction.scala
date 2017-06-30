package io.eels.datastream

import io.eels.Row

case class VectorAction(ds: DataStream) {
  def execute: Vector[Row] = ds.flows.map(_.iterator).reduce((a, b) => a ++ b).toVector
}
