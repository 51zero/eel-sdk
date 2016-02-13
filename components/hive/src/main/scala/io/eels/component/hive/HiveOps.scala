package io.eels.component.hive

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.hive.metastore.{TableType, HiveMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.{SerDeInfo, FieldSchema, StorageDescriptor, Table}
import scala.collection.JavaConverters._

object HiveOps extends StrictLogging {

  def createTable(databaseName: String, tableName: String, overwrite: Boolean = false)
                 (implicit client: HiveMetaStoreClient): Boolean = {

    if (overwrite) {
      logger.debug("Removing existing table")
      client.dropTable(databaseName, tableName)
    }

    val sd = new StorageDescriptor()
    sd.addToCols(new FieldSchema("id", "string", "Created by eel-sdk"))
    sd.setSerdeInfo(new SerDeInfo(null,
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
      Map("serialization.format" -> "1").asJava
    ))
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat")
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")

    val tab = new Table()
    tab.setDbName(databaseName)
    tab.setTableName(tableName)
    tab.setCreateTime((System.currentTimeMillis / 1000).toInt)
    tab.setSd(sd)
    tab.setPartitionKeys(new util.ArrayList[FieldSchema])
    tab.setTableType(TableType.MANAGED_TABLE.name())

    client.createTable(tab)
    true
  }
}
