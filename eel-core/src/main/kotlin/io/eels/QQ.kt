package io.eels

import java.util.concurrent.BlockingQueue
import java.util.stream.Stream

/**
 * A java stream that allows multiple, concurrent, streams to be created from
 */
class BlockingQueueConcurrentStream<E>(queue: BlockingQueue<E>, sentinel: E) : Stream<E> {

  private val iterator: Iterator<E>
  {
    Iterator.continually(queue.take).takeWhile { e ->
      if (e == sentinel)
        queue.put(e)
      e != sentinel
    }
  }

  override fun hasNext(): Boolean = iterator.hasNext()
  override fun next(): E = iterator.next()
}