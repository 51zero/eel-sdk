package io.eels.component.hive

import io.eels.component.parquet.ParquetLogMute
import io.eels.schema.Schema
import io.eels.util.zipWithIndex
import org.apache.hadoop.hive.metastore.IMetaStoreClient

class HiveDatabase(val dbName: String, val client: IMetaStoreClient) {
  fun tables(): List<HiveTable> {
    val tables = client.getAllTables(dbName)
    return tables.map { HiveTable(dbName, it, client) }
  }

  fun table(tableName: String): HiveTable {
    val exists = client.tableExists(dbName, tableName)
    if (!exists)
      throw IllegalArgumentException("$dbName.$tableName does not exist")
    return HiveTable(dbName, tableName, client)
  }
}

class HiveTable(val dbName: String, val tableName: String, val client: IMetaStoreClient) {
  init {
    ParquetLogMute()
  }

  private val ops = HiveOps(client)

  // returns the full underlying schema from the metastore including partition keys
  fun schema(): Schema = ops.schema(dbName, tableName)

  fun partitionKeys(): List<PartitionKey> {
    val keys = client.getTable(dbName, tableName).partitionKeys
    val parts = client.listPartitions(dbName, tableName, Short.MAX_VALUE)
    assert(keys.size == parts.size, { "Differing amount of keys to parts; possible bug" })
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
        PartitionKeyValue(key, value)
      }
      Partition(it.createTime * 1000L, it.sd, values)
    }
  }

  fun partitionNames(): List<String> = partitions().map { it.name() }

  override fun toString(): String {
    return "HiveTable(dbName='$dbName', tableName='$tableName')"
  }
}