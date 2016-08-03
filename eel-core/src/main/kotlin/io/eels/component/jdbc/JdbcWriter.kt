package io.eels.component.jdbc

import io.eels.Row
import io.eels.RowListener
import io.eels.SinkWriter
import io.eels.schema.Schema
import io.eels.util.Logging
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class JdbcWriter(schema: Schema,
                 url: String,
                 table: String,
                 createTable: Boolean,
                 dialect: JdbcDialect,
                 threads: Int,
                 batchSize: Int,
                 autoCommit: Boolean,
                 bufferSize: Int,
                 listener: RowListener) : SinkWriter, Logging {
  init {
    logger.info("Creating Jdbc writer with $threads threads, batch size $batchSize, autoCommit=$autoCommit")
    require(bufferSize >= batchSize)
  }

  // the buffer is a concurrent receiver for the write method. It needs to hold enough elements so that
  // the invokers of this class can keek pumping in rows while we wait for a buffer to fill up.
  // the buffer size must be >= batch size or we'll never fill up enough to trigger a batch
  private val buffer = LinkedBlockingQueue<Row>(bufferSize)

  // the coordinator pool is just a single thread that runs the coordinator
  private val coordinatorPool = Executors.newSingleThreadExecutor()

  private val inserter: JdbcInserter by lazy {
    val inserter = JdbcInserter(url, table, schema, autoCommit, dialect, listener)
    if (createTable) {
      inserter.ensureTableCreated()
    }
    inserter
  }

  init {
    // todo this needs to allow multiple batches at once
    coordinatorPool.submit {
      try {
        logger.debug("Starting JdbcWriter Coordinator")
        val toWrite = mutableListOf<Row>()
        var row: Row = buffer.take()
        // once we receive the pill its all over for the writer
        while (row != Row.PoisonPill) {
          toWrite.add(row)
          if (toWrite.size == batchSize) {
            inserter.insertBatch(toWrite)
            toWrite.clear()
          }
          row = buffer.take()
        }
        if (toWrite.isNotEmpty()) {
          inserter.insertBatch(toWrite)
          toWrite.clear()
        }
        logger.debug("Write completed; shutting down coordinator")
      } catch (e: Exception) {
        logger.error("Some error in coordinator", e)
      }
    }
    // the coordinate only runs the one task, that is to read from the buffer
    // and do the inserts
    coordinatorPool.shutdown()
  }

  override fun close(): Unit {
    buffer.put(Row.PoisonPill)
    logger.info("Closing JDBC Writer... waiting on writes to finish")
    coordinatorPool.awaitTermination(1, TimeUnit.DAYS)
  }

  // when we get a row to write, we won't commit it immediately to the database,
  // but we'll buffer it so we can do batched inserts
  override fun write(row: Row): Unit {
    buffer.put(row)
  }
}