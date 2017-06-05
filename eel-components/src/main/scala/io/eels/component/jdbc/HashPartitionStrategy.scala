package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Part

case class HashPartitionStrategy(hashExpression: String,
                                 numberOfPartitions: Int) extends JdbcPartitionStrategy {

  def partitionedQuery(partNum: Int, query: String): String =
    s"""SELECT * from ($query) WHERE $hashExpression = $partNum""".stripMargin

  override def parts(connFn: () => Connection,
                     query: String,
                     bindFn: (PreparedStatement) => Unit,
                     fetchSize: Int,
                     dialect: JdbcDialect): Seq[Part] = {

    for (k <- 0 until numberOfPartitions) yield {
      new JdbcPart(connFn, partitionedQuery(k, query), bindFn, fetchSize, dialect)
    }
  }
}
