package io.eels.component.hive

import io.eels.schema.Partition

case class HiveStats(dbName: String,
                     tableName: String,
                     // total number of rows in all partitions
                     rows: Long,
                     // map of partition name to various statistics
                     partitions: Map[Partition, PartitionStats])

case class PartitionStats(rows: Long)
