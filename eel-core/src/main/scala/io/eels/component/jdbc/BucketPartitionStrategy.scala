package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Chunk
import io.eels.component.jdbc.dialect.JdbcDialect
import io.eels.datastream.Publisher

case class BucketPartitionStrategy(columnName: String,
                                   values: Set[String]) extends JdbcPartitionStrategy {

  override def parts(connFn: () => Connection,
                     query: String,
                     bindFn: (PreparedStatement) => Unit,
                     fetchSize: Int,
                     dialect: JdbcDialect): Seq[Publisher[Chunk]] = {

    values.map { value =>
      val partitionedQuery = s""" SELECT * FROM ( $query ) WHERE $columnName = '$value' """
      new JdbcPublisher(connFn, partitionedQuery, bindFn, fetchSize, dialect)
    }.toSeq
  }
}



