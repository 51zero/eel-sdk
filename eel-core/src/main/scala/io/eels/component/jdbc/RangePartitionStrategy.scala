package io.eels.component.jdbc

import java.sql.{Connection, PreparedStatement}

import io.eels.Part

case class RangePartitionStrategy(columnName: String,
                                  numberOfPartitions: Int,
                                  min: Long,
                                  max: Long) extends JdbcPartitionStrategy {

  def ranges: Seq[(Long, Long)] = {

    // distribute surplus as evenly as possible across buckets
    // min max + 1 because the min-max range is inclusive
    val surplus = (max - min + 1) % numberOfPartitions
    val gap = (max - min + 1) / numberOfPartitions

    List.tabulate(numberOfPartitions) { k =>
      val start = min + k * gap + Math.min(k, surplus)
      val end = min + ((k + 1) * gap) + Math.min(k + 1, surplus)
      (start, end - 1)
    }
  }

  override def parts(connFn: () => Connection,
                     query: String,
                     bindFn: (PreparedStatement) => Unit,
                     fetchSize: Int,
                     dialect: JdbcDialect): Seq[Part] = {

    ranges.map { case (start, end) =>

      val partitionedQuery =
        s"""SELECT * FROM ( $query ) WHERE $start <= $columnName AND $columnName <= $end"""

      new JdbcPart(connFn, partitionedQuery, bindFn, fetchSize, dialect)
    }
  }
}



