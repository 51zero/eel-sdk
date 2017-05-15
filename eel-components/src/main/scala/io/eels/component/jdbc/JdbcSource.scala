package io.eels.component.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sksamuel.exts.Logging
import com.sksamuel.exts.io.Using
import com.sksamuel.exts.metrics.Timed
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Part, Row, Source}

object JdbcSource {
  def apply(url: String, query: String): JdbcSource = JdbcSource(() => DriverManager.getConnection(url), query)
}

case class JdbcSource(connFn: () => Connection,
                      query: String,
                      bind: (PreparedStatement) => Unit = stmt => (),
                      fetchSize: Int = 100,
                      providedSchema: Option[StructType] = None,
                      providedDialect: Option[JdbcDialect] = None,
                      bucketing: Option[Bucketing] = None)
  extends Source with JdbcPrimitives with Logging with Using with Timed {

  override lazy val schema: StructType = providedSchema.getOrElse(fetchSchema())

  def withBind(bind: (PreparedStatement) => Unit): JdbcSource = copy(bind = bind)
  def withFetchSize(fetchSize: Int): JdbcSource = copy(fetchSize = fetchSize)
  def withProvidedSchema(schema: StructType): JdbcSource = copy(providedSchema = Option(schema))
  def withProvidedDialect(dialect: JdbcDialect): JdbcSource = copy(providedDialect = Option(dialect))

  private def dialect(): JdbcDialect = providedDialect.getOrElse(new GenericJdbcDialect())

  override def parts(): List[JdbcPart] = List(new JdbcPart(connFn, query, bind, fetchSize, dialect()))

  def fetchSchema(): StructType = {
    using(connFn()) { conn =>
      val schemaQuery = s"SELECT * FROM ($query) tmp WHERE 1=0"
      using(conn.prepareStatement(schemaQuery)) { stmt =>

        stmt.setFetchSize(fetchSize)
        bind(stmt)

        val rs = timed("Executing query $query") {
          stmt.executeQuery()
        }

        val schema = schemaFor(dialect(), rs)
        rs.close()
        schema
      }
    }
  }
}

case class Bucketing(columnName: String, numberOfBuckets: Int)

class JdbcPart(connFn: () => Connection,
               query: String,
               bind: (PreparedStatement) => Unit = stmt => (),
               fetchSize: Int = 100,
               dialect: JdbcDialect
              ) extends Part with Timed with JdbcPrimitives {

  /**
    * Returns the data contained in this part in the form of an iterator. This function should return a new
    * iterator on each invocation. The iterator can be lazily initialized to the first read if required.
    */
  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    private val conn = connFn()
    private val stmt = conn.prepareStatement(query)
    stmt.setFetchSize(fetchSize)
    bind(stmt)

    private val rs = timed(s"Executing query $query") {
      stmt.executeQuery()
    }

    private val schema = schemaFor(dialect, rs)

    override def close(): Unit = {
      super.close()
      rs.close()
      conn.close()
    }

    override val iterator: Iterator[Seq[Row]] = new Iterator[Row] {

      var _hasnext = false

      override def hasNext(): Boolean = _hasnext || {
        _hasnext = rs.next()
        _hasnext
      }

      override def next(): Row = {
        _hasnext = false
        val values = schema.fieldNames().map(name => rs.getObject(name))
        Row(schema, values)
      }

    }.grouped(100).withPartial(true)
  }
}