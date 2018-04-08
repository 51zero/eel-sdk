package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Row
import io.eels.component.jdbc.dialect.JdbcDialect
import io.eels.datastream.Publisher

case class BucketExprPartitionStrategy(bucketExpressions: Seq[String]) extends JdbcPartitionStrategy {

  override def parts(connFn: () => Connection,
                     query: String,
                     bindFn: (PreparedStatement) => Unit,
                     fetchSize: Int,
                     dialect: JdbcDialect): Seq[Publisher[Seq[Row]]] = {

    bucketExpressions.map { bucketExpression =>
      val partitionedQuery = s""" SELECT * FROM ( $query ) WHERE $bucketExpression """
      new JdbcPublisher(connFn, partitionedQuery, bindFn, fetchSize, dialect)
    }
  }
}



