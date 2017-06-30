package io.eels.datastream

import io.eels.Row

case class IteratorAction(ds: DataStream) {
  def execute: Iterator[Row] = ds.flows.map(_.iterator).reduce((a, b) => a ++ b)
}
