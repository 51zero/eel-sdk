package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Part

trait JdbcPartitionStrategy {
  def parts(connFn: () => Connection,
            query: String,
            bindFn: (PreparedStatement) => Unit,
            fetchSize: Int,
            dialect: JdbcDialect): Seq[Part]
}
