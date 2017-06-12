package io.eels.dataframe

import io.eels.Row

case class IteratorAction(stream: DataStream) {
  def execute(): Iterator[Row] = null
}
