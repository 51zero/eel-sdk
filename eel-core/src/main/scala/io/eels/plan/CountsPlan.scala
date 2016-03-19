package io.eels.plan

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.io.Using
import io.eels._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object CountsPlan extends Plan with Using with Logging {

  private val DistinctValueCap = config.getInt("eel.plans.counts.distinctValueCap")

  type ColumnName = String
  type Value = Any
  type ValueCount = Long
  type Counts = mutable.Map[Value, ValueCount]

  def apply(frame: Frame)(implicit executor: ExecutionContext): Map[ColumnName, Counts] = {
    logger.info(s"Executing counts on frame [tasks=$tasks]")

    val buffer = frame.buffer
    val schema = frame.schema
    val latch = new CountDownLatch(tasks)
    val running = new AtomicBoolean(true)

    val futures = (1 to tasks).map { k =>
      Future {
        try {
          val maps = mutable.Map.empty[ColumnName, Counts]
          buffer.iterator.takeWhile(_ => running.get).foreach { row =>
            for ((value, columnName) <- row.zip(schema.columnNames)) {
              val counts = maps.getOrElseUpdate(columnName, mutable.Map.empty[Value, ValueCount])
              if (counts.contains(value) || counts.size < DistinctValueCap) {
                val count = counts.getOrElse(value, 0l)
                counts.put(value, count + 1)
              }
            }
          }
          maps.toMap
        } catch {
          case e: Throwable =>
            logger.error("Error reading; aborting tasks", e)
            running.set(false)
            throw e
        } finally {
          latch.countDown()
        }
      }
    }

    latch.await(timeout.toNanos, TimeUnit.NANOSECONDS)
    logger.debug("Closing buffer")
    buffer.close()
    logger.debug("Buffer closed")

    raiseExceptionOnFailure(futures)

    def combineValues(m1: Counts, m2: Counts): Counts = {
      // each map might have different values - if for example a value only existed once it would only appear in
      // one of these source sets
      m2.keys.foreach { key =>
        val total = m1.getOrElse(key, 0l) + m2.getOrElse(key, 0l)
        m1.put(key, total)
      }
      m1
    }

    def combineMaps(m1: Map[ColumnName, Counts], m2: Map[ColumnName, Counts]): Map[ColumnName, Counts] = {
      schema.columnNames.map { columnName =>
        columnName -> combineValues(m1(columnName), m2(columnName))
      }.toMap
    }

    val maps = Await.result(Future.sequence(futures), 1.minute)
    maps.reduceLeft((a, b) => combineMaps(a, b))
  }
}
