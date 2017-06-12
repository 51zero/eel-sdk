package io.eels

import java.util.function.{Consumer, LongConsumer}

import com.sksamuel.exts.Logging
import io.eels.dataframe.{DataStream, ExecutionManager}
import io.eels.schema.StructType
import io.eels.util.JacksonSupport
import reactor.core.publisher.{Flux, FluxSink}

import scala.util.control.NonFatal

/**
  * A Source is a provider of data.
  *
  * A source implementation must provide two methods:
  *
  * 1: schema() which returns an eel schema for the data source.
  *
  * 2: parts() which returns zero or more Part instances representing the data.
  *
  * A part instance is a subset of the data in a Source, and allows for concurrent
  * reading of that data. For example a part could be a single file in a multi-file source, or
  * a partition in a partitioned source.
  */
trait Source extends Logging {
  outer =>

  def schema: StructType

  def parts(): Seq[Part]

  def load[T: Manifest](): Seq[T] = {
    toFrame().toSeq().map { row =>
      val node = JsonRow(row)
      JacksonSupport.mapper.readerFor[T].readValue(node)
    }
  }

  def toFrame(): Frame = toFrame(NoopListener)
  def toFrame(_listener: Listener): Frame = new SourceFrame(this, _listener)

  def toDataStream(): DataStream = toDataStream(NoopListener)
  def toDataStream(listener: Listener): DataStream = new DataStream {
    override def schema: StructType = outer.schema
    override private[eels] def partitions(implicit em: ExecutionManager) = outer.parts().map { part =>

      val iterator = part.iterator().toVector.flatten.iterator

      Flux.create(new Consumer[FluxSink[Row]] {
        override def accept(sink: FluxSink[Row]): Unit = {
          logger.info(s"Accepting sink $sink")
          sink.onRequest(new LongConsumer {
            override def accept(req: Long): Unit = {
              try {
                logger.info(s"onRequest $req")
                iterator.take(if (req > Int.MaxValue) Int.MaxValue else req.toInt).foreach(sink.next)
                if (iterator.isEmpty)
                  sink.complete()
              } catch {
                case NonFatal(e) => sink.error(e)
              }
            }
          })
        }
      }, FluxSink.OverflowStrategy.BUFFER)
    }
  }
}