package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Row
import io.eels.component.jdbc.dialect.JdbcDialect
import io.eels.datastream.Publisher

trait JdbcPartitionStrategy {
  def parts(connFn: () => Connection,
            query: String,
            bindFn: (PreparedStatement) => Unit,
            fetchSize: Int,
            dialect: JdbcDialect): Seq[Publisher[Seq[Row]]]
}
