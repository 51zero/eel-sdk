package io.eels.component.jdbc

import io.eels.Row
import io.eels.SinkWriter
import io.eels.schema.Schema
import io.eels.util.BoundedThreadPoolExecutor
import io.eels.util.Logging
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class JdbcWriter(val schema: Schema,
                 val url: String,
                 val table: String,
                 val dialect: JdbcDialect,
                 val threads: Int,
                 val batchSize: Int,
                 val autoCommit: Boolean,
                 val bufferSize: Int,
                 val swallowExceptions: Boolean) : SinkWriter, Logging {
  init {
    logger.info("Creating Jdbc writer with $threads threads, batch size $batchSize, autoCommit=$autoCommit")
  }

  // the buffer is a concurrent receiver for the write method. It needs to hold enough elements to keep the
  // frame threads busy until the coordinator thread gets a time slice and empties it via its iterator.
  // controlled by eel.jdbc.sink.bufferSize
  private val buffer = LinkedBlockingQueue<Row>(bufferSize)

  // We use a bounded executor because otherwise the executor would very quickly fill up with pending tasks
  // for all rows in the source. Then effectively we would have loaded the entire frame into memory and stored it
  // inside the worker tasks.
  private val workers = BoundedThreadPoolExecutor(threads, threads)
  private val batchCount = AtomicLong(0)
  private val coordinator = Executors.newSingleThreadExecutor()
  private var inserter: JdbcInserter? = null

  init {
    coordinator.submit {
      logger.debug("Starting Jdbc Writer Coordinator")
//      BlockingQueueConcurrentIterator(buffer, Row.PoisonPill)
//          .grouped(props.batchSize)
//          .withPartial(true)
//          .foreach {
//            ensureInserterCreated()
//            workers.submit {
//              try {
//                val offset = batchCount.addAndGet(props.batchSize)
//                logger.debug("Inserting batch $offset / ? =>")
//                inserter.insertBatch(it)
//              } catch(t: Throwable) {
//                if (swallowExceptions) {
//                  logger.error("Exception when inserting batch; continuing", t)
//                } else {
//                  logger.error("Exception when inserting batch; aborting writers", t)
//                  workers.shutdownNow()
//                  coordinator.shutdownNow()
//                }
//              }
//            }
//          }
      logger.debug("End of buffer reached; shutting down workers")
      workers.shutdown()
    }

    // the coordinate only runs the one task, that is to read all the data from the buffer and populate worker jobs
    // so it can be shut down immediately after that ask is submitted
    coordinator.shutdown()
  }


  private fun ensureInserterCreated(): Unit {
    // coordinator is single threaded, so simple var with null is fine
//    if (inserter == null) {
//      inserter = JdbcInserter(url, table, schema, autoCommit, dialect)
//      if (props.createTable)
//        inserter.ensureTableCreated()
//    }
  }

  override fun close(): Unit {
    buffer.put(Row.PoisonPill)
    logger.debug("Closing JDBC Writer... blocking until workers completed")
    workers.awaitTermination(1, TimeUnit.DAYS)
  }

  override fun write(row: Row): Unit {
    buffer.put(row)
  }
}