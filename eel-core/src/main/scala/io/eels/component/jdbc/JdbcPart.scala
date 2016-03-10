package io.eels.component.jdbc

import java.sql.{Connection, ResultSet, Statement}
import java.util.concurrent.atomic.AtomicBoolean

import com.sksamuel.scalax.Logging
import io.eels.{InternalRow, Part, Schema, SourceReader}

class JdbcPart(rs: ResultSet, stmt: Statement, conn: Connection, schema: Schema) extends Part with Logging {

  override def reader: SourceReader = new SourceReader {

    override def close(): Unit = {
      logger.debug("Closing reader")
      rs.close()
      stmt.close()
      conn.close()
    }

    val created = new AtomicBoolean(false)
    val columnCount = schema.columns.size

    override def iterator: Iterator[InternalRow] = new Iterator[InternalRow] {
      require(!created.get, "!Cannot create more than one iterator for a jdbc source!")
      created.set(true)

      override def hasNext: Boolean = {
        val hasNext = rs.next()
        if (!hasNext) {
          logger.debug("Resultset is completed; closing stream")
          close()
        }
        hasNext
      }

      override def next: InternalRow = {
        schema.columnNames.map { columnName =>
          rs.getObject(columnName)
        }
      }
    }
  }
}
