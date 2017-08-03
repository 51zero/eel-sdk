package io.eels.datastream

import io.eels.Rec

case class IteratorAction(ds: DataStream) {
  def execute: Iterator[Rec] = ds.toVector.iterator
}
