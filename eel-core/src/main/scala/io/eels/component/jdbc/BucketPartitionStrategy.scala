package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Row
import io.eels.component.jdbc.dialect.JdbcDialect
import io.eels.datastream.Publisher

case class BucketPartitionStrategy(columnName: String,
                                   values: Set[String]) extends JdbcPartitionStrategy {

  override def parts(connFn: () => Connection,
                     query: String,
                     bindFn: (PreparedStatement) => Unit,
                     fetchSize: Int,
                     dialect: JdbcDialect): Seq[Publisher[Seq[Row]]] = {

    values.map { value =>
      val partitionedQuery = s""" SELECT * FROM ( $query ) WHERE $columnName = '$value' """
      new JdbcPublisher(connFn, partitionedQuery, bindFn, fetchSize, dialect)
    }.toSeq
  }
}



