package io.eels.datastream

import io.eels.Row
import scala.collection.JavaConverters._

case class VectorAction(ds: DataStream) {
  def execute: Vector[Row] = ds.flowable.toList.blockingGet().asScala.toVector
}
