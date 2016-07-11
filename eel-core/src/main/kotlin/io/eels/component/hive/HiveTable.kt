package io.eels.component.hive

import io.eels.component.parquet.ParquetLogMute
import io.eels.schema.Schema
import io.eels.util.zipWithIndex
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Partition as HivePartition

class HiveTable(val dbName: String, val tableName: String, val fs: FileSystem, val client: IMetaStoreClient) {
  init {
    ParquetLogMute()
  }

  private val ops = HiveOps(client)

  // returns the full underlying schema from the metastore including partitions
  fun schema(): Schema = ops.schema(dbName, tableName)

  fun partitionKeys(): List<PartitionKey> {
    val keys = client.getTable(dbName, tableName).partitionKeys
    val parts = client.listPartitions(dbName, tableName, Short.MAX_VALUE)
    assert(keys.size == parts.size, { "Differing amount of keys (${keys.size}) to parts (${parts.size})" })
    return keys.zip(parts).map {
      val field = HiveSchemaFns.fromHiveField(it.first, false).withPartition(true)
      PartitionKey(field, it.second.createTime * 1000L, it.second.parameters)
    }
  }

  fun partitions(): List<Partition> {
    val keys = partitionKeys()
    return client.listPartitions(dbName, tableName, Short.MAX_VALUE).map {
      val values = it.values.zipWithIndex().map {
        val key = keys[it.second]
        val value = it.first
        PartitionPart(key.field.name, value)
      }
      Partition(it.createTime * 1000L, it.sd, values)
    }
  }

  fun partitionNames(): List<String> = partitions().map { it.name() }

  fun movePartition(partition: String, location: String): Unit {

  }

  fun toSource(): HiveSource = HiveSource(dbName, tableName, fs = fs, client = client)

  override fun toString(): String {
    return "HiveTable(dbName='$dbName', tableName='$tableName')"
  }
}