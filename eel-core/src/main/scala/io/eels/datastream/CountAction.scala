package io.eels.datastream

case class CountAction(ds: DataStream) {
  def execute: Long = {
    val counts = ds.flows.map(_.iterator.size)
    if (counts.isEmpty) 0 else counts.sum
  }
}
