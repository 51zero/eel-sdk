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

  def createPartition(dbName: String,
                      tableName: String,
                      partitionName: String,
                      partitionValue: String)
                     (implicit client: HiveMetaStoreClient): Unit = {

    val existing = client.listPartitionNames(dbName, tableName, Short.MaxValue).asScala.map(_.toLowerCase)
    if (!existing.contains(s"$partitionName=$partitionValue".toLowerCase)) {
      logger.debug(s"Creating partition value '$partitionName=$partitionValue'")
      val table = client.getTable(dbName, tableName)

      val sd = new StorageDescriptor(table.getSd)
      sd.setLocation(table.getSd.getLocation + "/" + Partition(partitionName, partitionValue).unquotedDir)

      val part = new org.apache.hadoop.hive.metastore.api.Partition(
        new util.ArrayList,
        dbName,
        tableName,
        (System.currentTimeMillis / 1000).toInt,
        0,
        sd,
        new java.util.HashMap
      )
      part.addToValues(partitionValue.toLowerCase)
      client.add_partition(part)
    }
  }

  def createTable(databaseName: String,
                  tableName: String,
                  schema: FrameSchema,
                  partitionKey: List[String] = Nil,
                  format: HiveFormat = HiveFormat.Text,
                  props: Map[String, String] = Map.empty,
                  tableType: TableType = TableType.MANAGED_TABLE,
                  location: Option[String] = None,
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
      sd.setCols(HiveSchemaFieldsFn(schema.columns.filterNot(col => partitionKey.contains(col.name))).asJava)
      sd.setSerdeInfo(new SerDeInfo(
        null,
        format.serdeClass,
        Map("serialization.format" -> "1").asJava
      ))
      sd.setInputFormat(format.inputFormatClass)
      sd.setOutputFormat(format.outputFormatClass)
      location.foreach(sd.setLocation)

      val table = new Table()
      table.setDbName(databaseName)
      table.setTableName(tableName)
      table.setCreateTime((System.currentTimeMillis / 1000).toInt)
      table.setSd(sd)
      table.setPartitionKeys(partitionKey.map(new FieldSchema(_, "string", null)).asJava)
      table.setTableType(tableType.name)
      props.+("generated_by" -> "eel").foreach { case (key, value) => table.putToParameters(key, value) }

      client.createTable(table)
      logger.info(s"Table created $databaseName.$tableName")
      true
    } else {
      false
    }
  }
}
