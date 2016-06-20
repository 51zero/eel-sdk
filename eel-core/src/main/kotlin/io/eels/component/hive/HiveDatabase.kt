package io.eels.component.hive

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