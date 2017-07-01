package io.eels.datastream

case class CountAction(ds: DataStream) {
  def execute: Long = ds.flowable.count().blockingGet()
}
