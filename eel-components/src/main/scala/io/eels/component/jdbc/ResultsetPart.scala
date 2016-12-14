package io.eels.component.jdbc

import java.sql.{Connection, ResultSet, Statement}

import com.sksamuel.exts.Logging
import io.eels.schema.StructType
import io.eels.{CloseableIterator, Part, Row}

/**
  * A Part for a Resultset. Will publish all rows from the resultset and then close the resultset.
  */
class ResultsetPart(val rs: ResultSet,
                    val stmt: Statement,
                    val conn: Connection,
                    val schema: StructType) extends Part with Logging {

  /**
    * Returns the data contained in this part in the form of an iterator. This function should return a new
    * iterator on each invocation. The iterator can be lazily initialized to the first read if required.
    */
  override def iterator(): CloseableIterator[Seq[Row]] = new CloseableIterator[Seq[Row]] {

    override def close(): Unit = {
      super.close()
      rs.close()
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
