package io.eels.component.hive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

class HiveDatabase(val dbName: String, val fs: FileSystem, val client: IMetaStoreClient) {
  fun tables(): List<HiveTable> {
    val tables = client.getAllTables(dbName)
    return tables.map { HiveTable(dbName, it, fs, client) }
  }

  fun table(tableName: String): HiveTable {
    val exists = client.tableExists(dbName, tableName)
    if (!exists)
      throw IllegalArgumentException("$dbName.$tableName does not exist")
    return HiveTable(dbName, tableName, fs, client)
  }
}