package io.eels.datastream

import io.eels.Row

case class IteratorAction(stream: DataStream) {
  def execute: Iterator[Row] = stream.coalesce.iterator
}
