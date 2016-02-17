package io.eels.component.hive

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.FrameSchema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.{Partition => HivePartition, FieldSchema, SerDeInfo, StorageDescriptor, Table}
import org.apache.hadoop.hive.metastore.{HiveMetaStoreClient, TableType}

import scala.collection.JavaConverters._

object HiveOps extends StrictLogging {

  def partitions(dbName: String, tableName: String)(implicit client: HiveMetaStoreClient): List[HivePartition] = {
    client.listPartitions(dbName, tableName, Short.MaxValue).asScala.toList
  }

  def createTime: Int = (System.currentTimeMillis / 1000).toInt

  def partitionKeys(dbName: String, tableName: String)(implicit client: HiveMetaStoreClient): List[FieldSchema] = {
    client.getTable(dbName, tableName).getPartitionKeys.asScala.toList
  }

  def partitionKeyNames(dbName: String, tableName: String)(implicit client: HiveMetaStoreClient): List[String] = {
    partitionKeys(dbName, tableName).map(_.getName)
  }

  def tableExists(databaseName: String, tableName: String)(implicit client: HiveMetaStoreClient): Boolean = {
    client.tableExists(databaseName, tableName)
  }

  def tableFormat(dbName: String, tableName: String)(implicit client: HiveMetaStoreClient): String = {
    client.getTable(dbName, tableName).getSd.getInputFormat
  }

  def location(dbName: String, tableName: String)(implicit client: HiveMetaStoreClient): String = {
    client.getTable(dbName, tableName).getSd.getLocation
  }

  def tablePath(dbName: String, tableName: String)(implicit client: HiveMetaStoreClient): Path = {
    new Path(location(dbName, tableName))
  }

  def partitionPath(dbName: String, tableName: String, parts: Seq[PartitionPart])
                   (implicit client: HiveMetaStoreClient): Path = {
    partitionPath(dbName, tableName, parts, tablePath(dbName, tableName))
  }

  def partitionPath(dbName: String, tableName: String, parts: Seq[PartitionPart], tablePath: Path): Path = {
    parts.foldLeft(tablePath) { (path, part) => new Path(path, part.unquotedDir) }
  }

  // creates (if not existing) the partition for the given partition parts
  def partitionExists(dbName: String,
                      tableName: String,
                      parts: Seq[PartitionPart])
                     (implicit client: HiveMetaStoreClient): Boolean = {
    val partitionName = parts.map(_.unquotedDir).mkString("/")
    logger.debug(s"Checking if partition exists '$partitionName'")
    try {
      client.getPartition(dbName, tableName, partitionName) != null
    } catch {
      case _: Throwable => false
    }
  }

  // creates (if not existing) the partition for the given partition parts
  def createPartitionIfNotExists(dbName: String,
                                 tableName: String,
                                 parts: Seq[PartitionPart])
                                (implicit client: HiveMetaStoreClient): Unit = {
    val partitionName = parts.map(_.unquotedDir).mkString("/")
    logger.debug(s"Ensuring partition exists '$partitionName'")
    val exists = try {
      client.getPartition(dbName, tableName, partitionName) != null
    } catch {
      case _: Throwable => false
    }

    if (!exists) {

      val path = partitionPath(dbName, tableName, parts)
      logger.debug(s"Creating partition '$partitionName' at $path")

      val table = client.getTable(dbName, tableName)
      val sd = new StorageDescriptor(table.getSd)
      sd.setLocation(path.toString)

      val partition = new HivePartition(
        parts.map(_.value).toList.asJava,
        dbName,
        tableName,
        createTime,
        0,
        sd,
        new util.HashMap
      )

      client.add_partition(partition)
    }
  }

  def createTable(databaseName: String,
                  tableName: String,
                  schema: FrameSchema,
                  partitionKeys: List[String] = Nil,
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
      logger.info(s"Creating table $databaseName.$tableName with partitionKeys=${partitionKeys.mkString(",")}")

      val sd = new StorageDescriptor()
      sd.setCols(HiveSchemaFieldsFn(schema.columns.filterNot(col => partitionKeys.contains(col.name))).asJava)
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
      table.setCreateTime(createTime)
      table.setSd(sd)
      table.setPartitionKeys(partitionKeys.map(new FieldSchema(_, "string", null)).asJava)
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
