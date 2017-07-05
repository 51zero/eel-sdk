package io.eels.datastream

import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Listener, NoopListener, Row, Source}
import io.reactivex.Flowable
import io.reactivex.schedulers.Schedulers

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

// an implementation of DataStream that provides a Flowable populated from 1 or more parts
class DataStreamSource(source: Source, listener: Listener = NoopListener) extends DataStream with Using {

  override def schema: StructType = source.schema

  override def flowable: Flowable[Row] = {
    val parts = source.parts()
    if (parts.isEmpty) {
      Flowable.empty()
    } else {
      try {
        val flowables = parts.map(_.open.subscribeOn(Schedulers.io))
        Flowable.merge(flowables.asJava)
      } catch {
        case NonFatal(e) => Flowable.error(e)
      }
    }
  }
}