package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Part

case class HashPartitionStrategy(hashExpression: String,
                                 numberOfPartitions: Int) extends JdbcPartitionStrategy {

  def partitionedQuery(partNum: Int, query: String): String =
    s"""|SELECT *
        |FROM (
        |  SELECT eel_tmp.*, $hashExpression AS eel_hash_col
        |  FROM ( $query ) eel_tmp
        |)
        |WHERE eel_hash_col = $partNum
        |""".stripMargin

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
