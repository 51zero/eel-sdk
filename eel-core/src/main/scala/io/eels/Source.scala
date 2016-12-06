package io.eels

import java.util.concurrent._

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers

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

  def schema(): StructType

  def parts(): List[Part] = Nil

  def parts2(): List[Part2] = Nil

  def toFrame(): Frame = toFrame(NoopListener)

  def toFrame(ioThreads: Int): Frame = toFrame(Executors.newFixedThreadPool(ioThreads))

  def toFrame(executor: ExecutorService): Frame = toFrame(executor, NoopListener)

  def toFrame(listener: Listener): Frame = toFrame(Executors.newSingleThreadExecutor, listener)

  /**
    * Builds a frame from this source. The frame will be lazily loaded when an action is performed.
    *
    * @param ioThreads the number of threads to use in the source reader
    * @param observer  a listener for row items
    * @return a new frame
    */
  def toFrame(executor: ExecutorService, _listener: Listener): Frame = new Frame {

    override val schema: StructType = outer.schema()

    // this method may be invoked multiple times, each time generating a new "load action"
    override def rows(): Flux[Row] = {
      val fluxes = parts.map(_.data.subscribeOn(Schedulers.fromExecutorService(executor)))
      Flux.merge(fluxes: _*)
    }
  }.listener(_listener)

}