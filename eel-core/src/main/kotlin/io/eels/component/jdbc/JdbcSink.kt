package io.eels.component.jdbc

import com.typesafe.config.ConfigFactory
import io.eels.Logging
import io.eels.Row
import io.eels.Schema
import io.eels.Sink
import io.eels.SinkWriter
import org.apache.commons.beanutils.ResultSetIterator
import java.sql.DriverManager
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class JdbcSink(url: String, table: String, props: JdbcSinkProps = JdbcSinkProps()) : Sink, Logging {

  private val dialect = props.dialectFn(url)
  logger.debug("Dialect detected from url [$dialect]")

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.jdbc.sink.bufferSize")
  private val autoCommit = config.getBoolean("eel.jdbc.sink.autoCommit")
  private val warnIfMissingRewriteBatchedStatements = config.getBoolean("eel.jdbc.sink.warnIfMissingRewriteBatchedStatements")
  private val swallowExceptions = config.getBoolean("eel.jdbc.sink.swallowExceptions")

  if (!url.contains("rewriteBatchedStatements")) {
    if (warnIfMissingRewriteBatchedStatements) {
      logger.warn("JDBC connection string does not contain the property 'rewriteBatchedStatements=true' which can be a major performance boost when writing data via JDBC. " +
        "Add this property to your connection string, or to remove this warning set eel.jdbc.warnIfMissingRewriteBatchedStatements=false")
    }
  }

  override fun writer(schema: Schema) = JdbcWriter(schema, url, table, dialect, props, autoCommit, bufferSize, swallowExceptions)
}

class JdbcWriter(schema: Schema,
                 url: String,
                 table: String,
                 dialect: JdbcDialect,
                 props: JdbcSinkProps = JdbcSinkProps(),
                 autoCommit: Boolean,
                 bufferSize: Int,
                 swallowExceptions: Boolean) : SinkWriter, Logging {
  logger.info(s"Creating Jdbc writer with ${props.threads} threads, batch size ${props.batchSize}, autoCommit=$autoCommit")

  import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable

  // the buffer is a concurrent receiver for the write method. It needs to hold enough elements to keep the
  // frame threads busy until the coordinator thread gets a time slice and empties it via its iterator.
  // controlled by eel.jdbc.sink.bufferSize
  private val buffer = new LinkedBlockingQueue[InternalRow](bufferSize)

  // We use a bounded executor because otherwise the executor would very quickly fill up with pending tasks
  // for all rows in the source. Then effectively we would have loaded the entire frame into memory and stored it
  // inside the worker tasks.
  private val workers = new BoundedThreadPoolExecutor(props.threads, props.threads)
  private val batchCount = new AtomicLong(0)
  private val coordinator = Executors.newSingleThreadExecutor()
  private var inserter: JdbcInserter = null

  coordinator.submit {
    logger.debug("Starting Jdbc Writer Coordinator")
    BlockingQueueConcurrentIterator(buffer, InternalRow.PoisonPill)
      .grouped(props.batchSize)
      .withPartial(true)
      .foreach { batch =>
        ensureInserterCreated()
        workers.submit {
          try {
            val offset = batchCount.addAndGet(props.batchSize)
            logger.debug(s"Inserting batch $offset / ? =>")
            inserter.insertBatch(batch)
          } catch {
            case t: Throwable =>
              if (swallowExceptions) {
                logger.error("Exception when inserting batch; continuing", t)
              } else {
                logger.error("Exception when inserting batch; aborting writers", t)
                workers.shutdownNow()
                coordinator.shutdownNow()
              }
          }
        }
      }
    logger.debug("End of buffer reached; shutting down workers")
    workers.shutdown()
  }

  // the coordinate only runs the one task, that is to read all the data from the buffer and populate worker jobs
  // so it can be shut down immediately after that ask is submitted
  coordinator.shutdown()

  private fun ensureInserterCreated(): Unit {
    // coordinator is single threaded, so simple var with null is fine
    if (inserter == null) {
      inserter = new JdbcInserter(url, table, schema, autoCommit, dialect)
      if (props.createTable)
        inserter.ensureTableCreated()
    }
  }

  override fun close(): Unit {
    buffer.put(InternalRow.PoisonPill)
    logger.debug("Closing JDBC Writer... blocking until workers completed")
    workers.awaitTermination(1, TimeUnit.DAYS)
  }

  override fun write(row: InternalRow): Unit {
    buffer.put(row)
  }
}

class JdbcInserter(url: String,
                   table: String,
                   schema: Schema,
                   autoCommit: Boolean,
                   dialect: JdbcDialect) : Logging {

  logger.debug("Connecting to jdbc $url...")
  val conn = DriverManager.getConnection(url)
  conn.setAutoCommit(autoCommit)
  logger.debug("Connected to $url")

  fun insertBatch(batch: Seq<Row>): Unit {
    val stmt = conn.prepareStatement(dialect.insertQuery(schema, table))
    try {
      batch.foreach { row ->
        row.zipWithIndex foreach {
          (value, k) ->
          stmt.setObject(k + 1, value)
        }
        stmt.addBatch()
      }
      val result = stmt.executeBatch()
      logger.debug(s"Batch completed; ${result.length} rows updated")
      if (!autoCommit) conn.commit()
    } catch {
      case e: Exception =>
        logger.error("Batch failure", e)
        if (!autoCommit)
          Try {
            conn.rollback()
          }
        throw e
    } finally {
      stmt.close()
    }
  }

  fun ensureTableCreated(): Unit {
    logger.debug(s"Ensuring table [$table] is created")

    fun tableExists: Boolean {
      logger.debug("Fetching list of tables to detect if $table exists")
      val tables = ResultSetIterator(conn.getMetaData.getTables(null, null, null, Array("TABLE"))).toList
      val names = tables.map(_.apply(3).toLowerCase)
      val exists = names contains table.toLowerCase
      logger.debug("${tables.size} tables found; $table exists is $exists")
      return exists
    }

    if (!tableExists) {
      val sql = dialect.create(schema, table)
      logger.info(s"Creating table $table [$sql]")
      val stmt = conn.createStatement()
      try {
        stmt.executeUpdate(sql)
        if (!autoCommit) conn.commit()
      } catch {
        case e: Exception ->
          logger.error("Batch failure", e)
          if (!autoCommit)
            Try {
              conn.rollback()
            }
          throw e
      } finally {
        stmt.close()
      }
    }
  }
}

data class JdbcSinkProps(val createTable: Boolean = false,
                         val batchSize: Int = 10000,
                         val dialectFn: (String) -> JdbcDialect,
                         val threads: Int = 4)

