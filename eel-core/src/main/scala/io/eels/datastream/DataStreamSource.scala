package io.eels.datastream

import java.util.concurrent.{Executors, TimeUnit}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import io.eels.schema.StructType
import io.eels.{Row, Source}

// an implementation of DataStream that provides a subscribe powered by constitent parts
class DataStreamSource(source: Source) extends DataStream with Using with Logging {

  override def schema: StructType = source.schema

  override def subscribe(s: Subscriber[Seq[Row]]): Unit = {

    val publishers = source.parts()
    if (publishers.isEmpty) {
      logger.info("No parts for this source")
      s.subscribed(Subscription.empty)
      s.completed()
    } else {
      logger.info(s"Datastream has ${publishers.size} parts")
      val executor = Executors.newCachedThreadPool()
      Publisher.merge(publishers, Row.Sentinel)(executor).subscribe(s)
      executor.shutdown()
      executor.awaitTermination(999, TimeUnit.DAYS)
      logger.info("Datastream source has completed")
    }
  }
}
