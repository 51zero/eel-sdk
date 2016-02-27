package io.eels.component.hive

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.Schema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.{FieldSchema, SerDeInfo, StorageDescriptor, Table, Partition => HivePartition}
import org.apache.hadoop.hive.metastore.{HiveMetaStoreClient, TableType}

import scala.collection.JavaConverters._

object HiveOps extends StrictLogging {

  /**
    * Creates a new partition in Hive in the given database:table in the default location, which will be the
    * partition key values as a subdirectory of the table location. The values for the serialzation formats are
    * taken from the values for the table.
    */
  def createPartition(dbName: String, tableName: String, partition: Partition)
                     (implicit client: HiveMetaStoreClient): Unit = {
    val table = client.getTable(dbName, tableName)
    val location = new Path(table.getSd.getLocation, partition.dirName)
    createPartition(dbName, tableName, partition, location)
  }

  /**
    * Creates a new partition in Hive in the given database:table. The location of the partition must be
    * specified. If you want to use the default location then use the other variant that doesn't require the
    * location path. The values for the serialzation formats are taken from the values for the table.
    */
  def createPartition(dbName: String, tableName: String, partition: Partition, location: Path)
                     (implicit client: HiveMetaStoreClient): Unit = {

    // we fetch the table so we can copy the serde/format values from the table. It makes no sense
    // to store a partition with different serialization formats to other partitions.
    val table = client.getTable(dbName, tableName)
    val sd = new StorageDescriptor(table.getSd)
    sd.setLocation(location.toString)

    val newPartition = new HivePartition(
      partition.values.asJava,
      dbName,
      tableName,
      createTimeAsInt,
      0,
      sd,
      new util.HashMap
    )

    client.add_partition(newPartition)
  }

  def partitions(dbName: String, tableName: String)(implicit client: HiveMetaStoreClient): List[HivePartition] = {
    client.listPartitions(dbName, tableName, Short.MaxValue).asScala.toList
  }

  def createTimeAsInt: Int = (System.currentTimeMillis / 1000).toInt

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
  // todo fix this to not use the deprecated classes
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

      val partition = Partition(parts.map(part => PartitionKeyValue(part.key, part.value)).toList)
      createPartition(dbName, tableName, partition)
    }
  }

  def createTable(databaseName: String,
                  tableName: String,
                  schema: Schema,
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
      val fields = HiveSchemaFns.toHiveFields(schema.columns.filterNot(col => partitionKeys.contains(col.name))).asJava
      sd.setCols(fields)
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
      table.setCreateTime(createTimeAsInt)
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
