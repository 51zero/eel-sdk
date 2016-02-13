package io.eels.component.hive

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.FrameSchema
import org.apache.hadoop.hive.metastore.{TableType, HiveMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.{SerDeInfo, FieldSchema, StorageDescriptor, Table}
import scala.collection.JavaConverters._

object HiveOps extends StrictLogging {

  def tableFormat(dbName: String, tableName: String)
                 (implicit client: HiveMetaStoreClient): String = {
    client.getTable(dbName, tableName).getSd.getInputFormat
  }

  def location(dbName: String, tableName: String)
              (implicit client: HiveMetaStoreClient): String = client.getTable(dbName, tableName).getSd.getLocation

  def tableExists(databaseName: String, tableName: String)
                 (implicit client: HiveMetaStoreClient): Boolean = {
    client.tableExists(databaseName, tableName)
  }

  def createTable(databaseName: String,
                  tableName: String,
                  schema: FrameSchema,
                  overwrite: Boolean = false)
                 (implicit client: HiveMetaStoreClient): Boolean = {

    if (overwrite) {
      logger.debug("Removing table if exists (overwrite mode = true)")
      if (tableExists(databaseName, tableName))
        client.dropTable(databaseName, tableName, true, true, true)
    }

    if (!tableExists(databaseName, tableName)) {
      logger.info(s"Creating table $databaseName.$tableName")

      val sd = new StorageDescriptor()
      sd.addToCols(new FieldSchema("id", "string", "Created by eel-sdk"))
      sd.setSerdeInfo(new SerDeInfo(null,
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        Map("serialization.format" -> "1").asJava
      ))
      sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat")
      sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")

      val table = new Table()
      table.setDbName(databaseName)
      table.setTableName(tableName)
      table.setCreateTime((System.currentTimeMillis / 1000).toInt)
      table.setSd(sd)
      table.setPartitionKeys(new util.ArrayList[FieldSchema])
      table.setTableType(TableType.MANAGED_TABLE.name)

      client.createTable(table)
      logger.info(s"Table created $databaseName.$tableName")
      true
    } else {
      false
    }
  }
}
