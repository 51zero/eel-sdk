package io.eels.actions

import io.eels._

object SinkAction extends Action {

  def execute(sink: Sink, frame: Frame, listener: Listener): Long = {

    val schema = frame.schema
    val writer = sink.writer(schema)
    var count = 0

    frame.rows().foreach { row =>
      writer.write(row)
      listener.onNext(row)
      count = count + 1
    }

    writer.close()
    listener.onComplete()

    logger.info("Sink plan completed")
    count
  }
}