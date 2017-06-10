package io.eels.component.jdbc

import java.sql.Connection
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{Row, SinkWriter}

class JdbcWriter(schema: StructType,
                 connFn: () => Connection,
                 table: String,
                 createTable: Boolean,
                 dialect: JdbcDialect,
                 threads: Int,
                 batchSize: Int,
                 autoCommit: Boolean,
                 bufferSize: Int) extends SinkWriter with Logging {
  logger.info("Creating Jdbc writer with $threads threads, batch size $batchSize, autoCommit=$autoCommit")
  require(bufferSize >= batchSize)

  // the buffer is a concurrent receiver for the write method. It needs to hold enough elements so that
  // the invokers of this class can keek pumping in rows while we wait for a buffer to fill up.
  // the buffer size must be >= batch size or we'll never fill up enough to trigger a batch
  private val buffer = new LinkedBlockingQueue[Row](bufferSize)

  // the coordinator pool is just a single thread that runs the coordinator
  private val coordinatorPool = Executors.newSingleThreadExecutor()

  private lazy val inserter = {
    val inserter = new JdbcInserter(connFn, table, schema, autoCommit, dialect)
    if (createTable) {
      inserter.ensureTableCreated()
    }
    inserter
  }

  import com.sksamuel.exts.concurrent.ExecutorImplicits._

  // todo this needs to allow multiple batches at once
  coordinatorPool.submit {
    try {
      logger.debug("Starting JdbcWriter Coordinator")
      // once we receive the pill its all over for the writer
      Iterator.continually(buffer.take)
        .takeWhile(_ != Row.Sentinel)
        .grouped(batchSize).withPartial(true)
        .foreach { batch =>
          inserter.insertBatch(batch)
      }
      logger.debug("Write completed; shutting down coordinator")
    } catch {
      case t: Throwable =>
        logger.error("Some error in coordinator", t)
    }
  }
  // the coordinate only runs the one task, that is to read from the buffer
  // and do the inserts
  coordinatorPool.shutdown()

  override def close(): Unit = {
    buffer.put(Row.Sentinel)
    logger.info("Closing JDBC Writer... waiting on writes to finish")
    coordinatorPool.awaitTermination(1, TimeUnit.DAYS)
  }

  // when we get a row to write, we won't commit it immediately to the database,
  // but we'll buffer it so we can do batched inserts
  override def write(row: Row): Unit = {
    buffer.put(row)
  }
}