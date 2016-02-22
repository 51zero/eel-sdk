package io.eels.component.jdbc

import java.sql.DriverManager
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import com.sksamuel.scalax.collection.BlockingQueueConcurrentIterator
import com.sksamuel.scalax.jdbc.ResultSetIterator
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{FrameSchema, Row, Sink, Writer}

import scala.language.implicitConversions
import scala.util.Try

case class JdbcSink(url: String, table: String, props: JdbcSinkProps = JdbcSinkProps())
  extends Sink
    with StrictLogging {

  private val dialect = props.dialectFn(url)
  logger.debug(s"Dialect detected from url [$dialect]")

  private val config = ConfigFactory.load()
  private val bufferSize = config.getInt("eel.jdbc.sink.bufferSize")
  private val autoCommit = config.getBoolean("eel.jdbc.sink.autoCommit")
  private val warnIfMissingRewriteBatchedStatements = config.getBoolean("eel.jdbc.sink.warnIfMissingRewriteBatchedStatements")

  if (!url.contains("rewriteBatchedStatements")) {
    if (warnIfMissingRewriteBatchedStatements) {
      logger.warn("JDBC connection string does not contain the property 'rewriteBatchedStatements=true' which can be a major performance boost when writing data via JDBC. " +
        "Add this property to your connection string, or to remove this warning set eel.jdbc.warnIfMissingRewriteBatchedStatements=false")
    }
  }

  override def writer = new JdbcWriter(url, table, dialect, props, autoCommit, bufferSize)
}

class BoundedThreadPoolExecutor(poolSize: Int, queueSize: Int)
  extends ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable]())
    with AutoCloseable {

  val semaphore = new Semaphore(poolSize + queueSize)
  val running = new AtomicBoolean(true)

  def execute(task: => Any): Unit = execute(new Runnable {
    override def run(): Unit = task
  })

  override def execute(runnable: Runnable): Unit = {

    var acquired = false
    while (running.get && !acquired) {
      try {
        semaphore.acquire()
        acquired = true
      } catch {
        case e: InterruptedException =>
      }
    }

    try {
      super.execute(runnable)
    } catch {
      case e: RejectedExecutionException =>
        semaphore.release()
        throw e;
    }
  }

  override def afterExecute(r: Runnable, t: Throwable): Unit = {
    super.afterExecute(r, t)
    semaphore.release()
  }

  override def close(): Unit = {
    running.set(false)
  }
}

class JdbcWriter(url: String,
                 table: String,
                 dialect: JdbcDialect,
                 props: JdbcSinkProps = JdbcSinkProps(),
                 autoCommit: Boolean = false,
                 bufferSize: Int) extends Writer with StrictLogging {
  logger.info(s"Creating Jdbc writer with ${props.threads} threads, batche size ${props.batchSize}, autoCommit=$autoCommit")

  import com.sksamuel.scalax.concurrent.ThreadImplicits.toRunnable

  // the buffer is a concurrent receiver for the write method. It needs to hold enough elements to keep the
  // frame threads busy until the coordinator thread gets a time slice and empties it via its iterator.
  // controlled by eel.jdbc.sink.bufferSize
  private val buffer = new ArrayBlockingQueue[Row](bufferSize)

  // We use a bounded executor because otherwise the executor would very quickly fill up with pending tasks
  // for all rows in the source. Then effectively we would have loaded the entire frame into memory and stored it
  // inside the worker tasks.
  private val workers = new BoundedThreadPoolExecutor(props.threads, props.threads)
  private val batchCount = new AtomicLong(0)
  private val coordinator = Executors.newSingleThreadExecutor()
  private val schemaRef = new AtomicReference[FrameSchema](null)
  private var inserter: JdbcInserter = null

  coordinator.submit {
    logger.debug("Starting Jdbc Writer Coordinator")
    BlockingQueueConcurrentIterator(buffer, Row.Sentinel)
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
              logger.error("Exception when inserting batch; aborting writers", t)
              workers.shutdownNow()
              coordinator.shutdownNow()
          }
        }
      }
    logger.debug("End of buffer reached; shutting down workers")
    workers.shutdown()
  }

  // the coordinate only runs the one task, that is to read all the data from the buffer and populate worker jobs
  // so it can be shut down immediately after that ask is submitted
  coordinator.shutdown()

  private def ensureInserterCreated(): Unit = {
    // coordinator is single threaded, so simple var with null is fine
    if (inserter == null) {
      inserter = new JdbcInserter(url, table, schemaRef.get, autoCommit, dialect)
      if (props.createTable)
        inserter.ensureTableCreated()
    }
  }

  override def close(): Unit = {
    buffer.put(Row.Sentinel)
    logger.debug("Closing JDBC Writer... blocking until workers completed")
    workers.awaitTermination(1, TimeUnit.DAYS)
  }

  override def write(row: Row, schema: FrameSchema): Unit = {
    // need an atomic ref here as this method will be called by multiple threads, and we need
    // to ensure the schema is visible to the coordinator thread when it calls ensureInserterCreated()
    if (schemaRef.get == null) schemaRef.set(schema)
    buffer.put(row)
  }
}

class JdbcInserter(url: String,
                   table: String,
                   schema: FrameSchema,
                   autoCommit: Boolean,
                   dialect: JdbcDialect) extends StrictLogging {

  logger.debug(s"Connecting to jdbc $url...")
  val conn = DriverManager.getConnection(url)
  conn.setAutoCommit(autoCommit)
  logger.debug(s"Connected to $url")

  def insertBatch(batch: Seq[Row]): Unit = {
    val stmt = conn.createStatement()
    try {
      batch.map(dialect.insert(_, schema, table)).foreach(stmt.addBatch)
      stmt.executeBatch()
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

  def ensureTableCreated(): Unit = {
    logger.debug(s"Ensuring table [$table] is created")

    def tableExists: Boolean = {
      logger.debug(s"Fetching list of tables to detect if $table exists")
      val tables = ResultSetIterator(conn.getMetaData.getTables(null, null, null, Array("TABLE"))).toList
      val names = tables.map(_.apply(3).toLowerCase)
      val exists = names contains table.toLowerCase
      logger.debug(s"${tables.size} tables found; $table exists is $exists")
      exists
    }

    if (!tableExists) {
      val sql = dialect.create(schema, table)
      logger.info(s"Creating table $table [$sql]")
      val stmt = conn.createStatement()
      try {
        stmt.executeUpdate(sql)
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
  }
}

case class JdbcSinkProps(createTable: Boolean = false,
                         batchSize: Int = 10000,
                         dialectFn: String => JdbcDialect = url => JdbcDialect(url),
                         threads: Int = 4)

