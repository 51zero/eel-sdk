package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Part
import io.eels.component.jdbc.dialect.JdbcDialect

case class BucketPartitionStrategy(columnName: String,
                                   values: Set[String]) extends JdbcPartitionStrategy {

  override def parts(connFn: () => Connection,
                     query: String,
                     bindFn: (PreparedStatement) => Unit,
                     fetchSize: Int,
                     dialect: JdbcDialect): Seq[Part] = {

    values.map { value =>
      val partitionedQuery = s""" SELECT * FROM ( $query ) WHERE $columnName = '$value' """
      new JdbcPart(connFn, partitionedQuery, bindFn, fetchSize, dialect)
    }.toSeq
  }
}



