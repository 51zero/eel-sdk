package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Chunk
import io.eels.component.jdbc.dialect.JdbcDialect
import io.eels.datastream.Publisher

case object SinglePartitionStrategy extends JdbcPartitionStrategy {
  override def parts(connFn: () => Connection,
                     query: String,
                     bindFn: (PreparedStatement) => Unit,
                     fetchSize: Int,
                     dialect: JdbcDialect): Seq[Publisher[Chunk]] = {
    List(new JdbcPublisher(connFn, query, bindFn, fetchSize, dialect))
  }
}
