package io.eels.component.hive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import scala.collection.JavaConverters._

case class HiveDatabase(dbName: String)(implicit fs: FileSystem, client: IMetaStoreClient) {
  def tables(): List[HiveTable] = {
    val tables = client.getAllTables(dbName).asScala
    tables.map { it => HiveTable(dbName, it) }.toList
  }

  def table(tableName: String): HiveTable = {
    val exists = client.tableExists(dbName, tableName)
    if (!exists)
      throw new IllegalArgumentException(s"$dbName.$tableName does not exist")
    HiveTable(dbName, tableName)
  }
}