package io.eels.datastream

import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Listener, NoopListener, Row, Source}
import io.reactivex.Flowable

import scala.collection.JavaConverters._

// an implementation of DataStream that provides a flux populated from 1 or more parts
class DataStreamSource(source: Source, listener: Listener = NoopListener) extends DataStream with Using {

  override def schema: StructType = source.schema

  override def flowable: Flowable[Row] = {

    val parts = source.parts()
    if (parts.isEmpty) {
      Flowable.empty()
    } else {
      // we buffer the reading from the sources so that slow io can constantly be performing in the background
      val flowables = parts.map(_.open)
      Flowable.merge(flowables.asJava)
    }
  }
}