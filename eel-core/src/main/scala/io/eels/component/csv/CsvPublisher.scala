package io.eels.component.csv

import java.io.InputStream
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import com.univocity.parsers.csv.CsvParser
import io.eels.Row
import io.eels.datastream.{DataStream, Publisher, Subscriber, Subscription}
import io.eels.schema.StructType

class CsvPublisher(createParser: () => CsvParser,
                   inputFn: () => InputStream,
                   header: Header,
                   schema: StructType) extends Publisher[Seq[Row]] with Logging with Using {

  val rowsToSkip: Int = header match {
    case Header.FirstRow => 1
    case _ => 0
  }

  override def subscribe(subscriber: Subscriber[Seq[Row]]): Unit = {

    val input = inputFn()
    val parser = createParser()

    try {
      parser.beginParsing(input)

      val running = new AtomicBoolean(true)
      subscriber.subscribed(Subscription.fromRunning(running))

      logger.debug(s"CSV Source will skip $rowsToSkip rows")

      val count = new AtomicLong(0)

      Iterator.continually(parser.parseNext)
        .takeWhile(_ != null)
        .takeWhile(_ => running.get)
        .drop(rowsToSkip)
        .map { record => Row(schema, record.toVector) }
        .grouped(DataStream.DefaultBatchSize)
        .foreach { ts =>
          count.addAndGet(ts.size)
          subscriber.next(ts)
        }

      logger.debug(s"All ${count.get} rows read, notifying subscriber")
      subscriber.completed()

    } catch {
      case t: Throwable =>
        logger.error(s"Error in CSV Source, subscriber will be notified", t)
        subscriber.error(t)

    } finally {
      logger.debug("Closing CSV source resources")
      parser.stopParsing()
    }
  }
}