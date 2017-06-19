package io.eels.actions

import io.eels.datastream.DataStream

case class CountAction(ds: DataStream) extends Action {
  def execute: Long = ds.partitions.foldLeft(0) { (count, ci) => count + ci.iterator.size }
}