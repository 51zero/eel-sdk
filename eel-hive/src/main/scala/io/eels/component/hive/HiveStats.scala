package io.eels.component.hive

case class HiveStats(dbName: String,
                     tableName: String,
                     // total number of rows in all partitions
                     rows: Long,
                     // map of partition name to various statistics
                     stats: Map[String, PartitionStats])

case class PartitionStats(rows: Long)
