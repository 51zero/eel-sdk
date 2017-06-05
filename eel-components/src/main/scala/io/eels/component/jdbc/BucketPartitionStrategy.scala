package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Part

case class BucketPartitionStrategy(columnName: String,
                                   numberOfPartitions: Int,
                                   min: Int,
                                   max: Int) extends JdbcPartitionStrategy {

  def ranges: Seq[Range] = {

    // distribute surplus as evenly as possible across buckets
    // min max + 1 because the min-max range is inclusive
    val surplus = (max - min + 1) % numberOfPartitions
    val gap = (max - min + 1) / numberOfPartitions

    List.tabulate(numberOfPartitions) { k =>
      val start = min + k * gap + Math.min(k, surplus)
      val end = min + ((k + 1) * gap) + Math.min(k + 1, surplus)
      Range(start, end)
    }
  }

  override def parts(connFn: () => Connection,
                     query: String,
                     bindFn: (PreparedStatement) => Unit,
                     fetchSize: Int,
                     dialect: JdbcDialect): Seq[Part] = {

    ranges.map { range =>

      val partitionedQuery =
        s"""|SELECT *
            |FROM (
            |  SELECT *
            |  FROM ( $query )
            |)
            |WHERE ${range.start} <= $columnName AND $columnName <= ${range.end}
            |""".stripMargin

      new JdbcPart(connFn, partitionedQuery, bindFn, fetchSize, dialect)
    }
  }
}



