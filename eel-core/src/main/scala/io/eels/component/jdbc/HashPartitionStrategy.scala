package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Chunk
import io.eels.component.jdbc.dialect.JdbcDialect
import io.eels.datastream.Publisher

case class HashPartitionStrategy(hashExpression: String,
                                 numberOfPartitions: Int) extends JdbcPartitionStrategy {

  def partitionedQuery(partNum: Int, query: String): String =
    s"""SELECT * from ($query) WHERE $hashExpression = $partNum""".stripMargin

  override def parts(connFn: () => Connection,
                     query: String,
                     bindFn: (PreparedStatement) => Unit,
                     fetchSize: Int,
                     dialect: JdbcDialect): Seq[Publisher[Chunk]] = {

    for (k <- 0 until numberOfPartitions) yield {
      new JdbcPublisher(connFn, partitionedQuery(k, query), bindFn, fetchSize, dialect)
    }
  }
}
