package io.eels

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import rx.lang.scala.Observer

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

  /**
    * Builds a frame from this source. The frame will be lazily loaded when an action is performed.
    *
    * @param ioThreads the number of threads to use in the source reader
    * @param observer  a listener for row items
    * @return a new frame
    */
  def toFrame(ioThreads: Int, observer: Observer[Row] = NoopObserver): Frame =
    new FrameSource(ioThreads, this, observer)

  def schema(): StructType
  def parts(): List[Part]
}