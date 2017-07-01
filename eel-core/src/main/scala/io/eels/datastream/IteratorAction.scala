package io.eels.datastream

import io.eels.Row

case class IteratorAction(ds: DataStream) {
  def execute: Iterator[Row] = Iterator.empty
}
