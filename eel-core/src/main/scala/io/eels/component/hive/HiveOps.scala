package io.eels.component.hive

import java.util

import com.sksamuel.exts.Logging
import io.eels.{Constants, PartitionPart, PartitionSpec}
import io.eels.schema.Field
import io.eels.schema.Schema
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.api.{Partition => HivePartition}

import scala.collection.JavaConverters._

class HiveOps(val client: IMetaStoreClient) extends Logging {

  //  /**
  //   * Returns a map of all partition partitionKeys to their values.
  //   * This operation is optimized, in that it does not need to scan files, but can retrieve the information
  //   * directly from the hive metastore.
  //   */
  //  def partitionMap(dbName: String, tableName: String): Map<String, List<String>> =
  //    client.listPartitionNames(dbName, tableName, Short.MAX_VALUE)
  //        .flatMap { Partition(it).parts }
  //        .groupBy { it.key }
  //        .map { key, values -> Pair(key, values.map { it.value }) }
  //
  //  /**
  //   * Returns all partition values for the given partition partitionKeys.
  //   * This operation is optimized, in that it does not need to scan files, but can retrieve the information
  //   * directly from the hive metastore.
  //   */
  //  def partitionValues(dbName: String, tableName: String, partitionKeys: List<String>): List<PartitionPartValues> =
  //    partitionMap(dbName, tableName).collect { case (key, values) if partitionKeys contains key => values }.toList

  //  /**
  //   * Returns all partition values for a given partition key.
  //   * This operation is optimized, in that it does not need to scan files, but can retrieve the information
  //   * directly from the hive metastore.
  //   */
  //  def partitionValues(dbName: String, tableName: String, key: String): List<String> =
  //      partitionMap(dbName, tableName).getOrElse(key, { listOf() })

  /**
    * Creates a new partition in Hive in the given database:table in the default location, which will be the
    * partition key values as a subdirectory of the table location. The values for the serialzation formats are
    * taken from the values for the table.
    */
  def createPartition(dbName: String, tableName: String, partition: PartitionSpec): Unit = {
    val table = client.getTable(dbName, tableName)
    val location = new Path(table.getSd.getLocation, partition.name())
    createPartition(dbName, tableName, partition, location)
  }

  /**
    * Creates a new partition in Hive in the given database:table. The location of the partition must be
    * specified. If you want to use the default location then use the other variant that doesn't require the
    * location path. The values for the serialzation formats are taken from the values for the table.
    */
  def createPartition(dbName: String, tableName: String, partition: PartitionSpec, location: Path): Unit = {

    // we fetch the table so we can copy the serde/format values from the table. It makes no sense
    // to store a partition with different serialization formats to other partitions.
    val table = client.getTable(dbName, tableName)
    val sd = new StorageDescriptor(table.getSd)
    sd.setLocation(location.toString())

    val vals = util.Arrays.asList(partition.values(): _*) // the hive partition values are the actual values of the partition parts
    val hivePartition = new HivePartition(
      vals,
      dbName,
      tableName,
      createTimeAsInt(),
      0,
      sd,
      new java.util.HashMap()
    )

    client.add_partition(hivePartition)
  }

  /**
    * Returns hive API partitions for the given dbName:tableName
    */
  def partitions(dbName: String, tableName: String): List[HivePartition] = client.listPartitions(dbName, tableName, Short.MaxValue).asScala.toList

  def createTimeAsInt(): Int = (System.currentTimeMillis() / 1000).toInt

  /**
    * Returns the hive FieldSchema's for partition columns.
    * Hive calls these "partition partitionKeys"
    */
  def partitionKeys(dbName: String, tableName: String): List[FieldSchema] = client.getTable(dbName, tableName).getPartitionKeys.asScala.toList

  def partitionKeyNames(dbName: String, tableName: String): List[String] = partitionKeys(dbName, tableName).map(_.getName)

  def tableExists(databaseName: String, tableName: String): Boolean = client.tableExists(databaseName, tableName)

  def tableFormat(dbName: String, tableName: String): String = client.getTable(dbName, tableName).getSd.getInputFormat

  def location(dbName: String, tableName: String): String = client.getTable(dbName, tableName).getSd.getLocation

  def tablePath(dbName: String, tableName: String): Path = new Path(location(dbName, tableName))

  def partitionPath(dbName: String, tableName: String, parts: List[PartitionPart]): Path =
    partitionPath(parts, tablePath(dbName, tableName))

  def partitionPath(parts: List[PartitionPart], tablePath: Path): Path = new Path(partitionPathString(parts, tablePath))

  def partitionPathString(parts: List[PartitionPart], tablePath: Path): String =
    tablePath.toString() + "/" + parts.map(_.unquoted).mkString("/")

  // Returns the eel schema for the hive dbName:tableName
  def schema(dbName: String, tableName: String): Schema = {
    val table = client.getTable(dbName, tableName)

    // hive columns are always nullable, and hive partitions are never nullable so we can set
    // the nullable fields appropriately
    val cols = table.getSd.getCols.asScala.map { it => HiveSchemaFns.fromHiveField(it, true) }
    val partitions = table.getPartitionKeys.asScala.map { it =>
      HiveSchemaFns.fromHiveField(it, false)
    }.map(_.withPartition(true))

    val columns = cols ++ partitions
    Schema(columns.toList)
  }

  /**
    * Adds this column to the hive schema. This is schema evolution.
    * The column must be marked as nullable and cannot have the same name as an existing column.
    */
  def addColumn(dbName: String, tableName: String, field: Field): Unit = {
    val table = client.getTable(dbName, tableName)
    val sd = table.getSd
    sd.addToCols(HiveSchemaFns.toHiveField(field))
    client.alter_table(dbName, tableName, table)
  }

  // creates (if not existing) the partition for the given partition parts
  def partitionExists(dbName: String,
                      tableName: String,
                      parts: List[PartitionPart]): Boolean = {

    val partitionName = parts.map(_.unquoted).mkString("/")
    logger.debug(s"Checking if partition exists '$partitionName'")

    try {
      client.getPartition(dbName, tableName, partitionName) != null
    } catch {
      case t: Throwable => false
    }
  }

  //  def applySpec(spec: HiveSpec, overwrite: Boolean): Unit {
  //    spec.tables().fore {
  //      val schemas = HiveSpecFn.toSchemas(spec)
  //      createTable(spec.dbName,
  //        table.tableName,
  //        schemas(table.tableName),
  //        table.partitionKeys,
  //        HiveFormat.fromInputFormat(table.inputFormat),
  //        Map.empty,
  //        TableType.MANAGED_TABLE,
  //        None,
  //        overwrite
  //      )
  //    }
  //  }

  // creates (if not existing) the partition for the given partition parts
  def createPartitionIfNotExists(dbName: String,
                                 tableName: String,
                                 parts: List[PartitionPart]): Unit = {
    val partitionName = parts.map(_.unquoted()).mkString("/")
    logger.debug(s"Ensuring partition exists '$partitionName'")
    val exists = try {
      client.getPartition(dbName, tableName, partitionName) != null
    } catch {
      case t: Throwable => false
    }

    if (!exists) {

      val path = partitionPath(dbName, tableName, parts)
      logger.debug(s"Creating partition '$partitionName' at $path")

      val partition = PartitionSpec(parts.toArray)
      createPartition(dbName, tableName, partition)
    }
  }

  def createTable(databaseName: String,
                  tableName: String,
                  schema: Schema,
                  partitionKeys: List[String],
                  format: HiveFormat = HiveFormat.Text,
                  props: Map[String, String] = Map.empty,
                  tableType: TableType = TableType.MANAGED_TABLE,
                  location: String = null,
                  overwrite: Boolean = false): Boolean = {
    for (partitionKey <- partitionKeys) {
      if (!schema.contains(partitionKey)) {
        throw new IllegalArgumentException(s"Schema must define all partition partitionKeys but it does not define $partitionKey")
      }
    }

    if (overwrite) {
      logger.debug(s"Removing table $databaseName.$tableName if exists (overwrite mode = true)")
      if (tableExists(databaseName, tableName)) {
        logger.debug(s"Table $databaseName.$tableName  exists, it will be dropped")
        client.dropTable(databaseName, tableName, true, true, true)
      } else {
        logger.debug(s"Table $databaseName.$tableName does not exist")
      }
    }

    if (!tableExists(databaseName, tableName)) {
      logger.info(s"Creating table $databaseName.$tableName with partitionKeys=${partitionKeys.mkString(",")}")

      // we will normalize all our columns as lower case, which is how hive treats them
      val lowerPartitionKeys = partitionKeys.map(_.toLowerCase)
      val lowerColumns = schema.fields.map(_.toLowerCase)

      val sd = new StorageDescriptor()

      // hive expects that the table fields will not contain partition partitionKeys
      sd.setCols(lowerColumns.filterNot { it => lowerPartitionKeys.contains(it.name) }.map(HiveSchemaFns.toHiveField).asJava)
      sd.setSerdeInfo(new SerDeInfo(
        null,
        format.serdeClass(),
        Map("serialization.format" -> "1").asJava
      ))
      sd.setInputFormat(format.inputFormatClass)
      sd.setOutputFormat(format.outputFormatClass)
      sd.setLocation(location)

      val table = new Table()
      table.setDbName(databaseName)
      table.setTableName(tableName)
      table.setCreateTime(createTimeAsInt())
      table.setSd(sd)
      // todo support non string partitions
      table.setPartitionKeys(lowerPartitionKeys.map { it => new FieldSchema(it, "string", null) }.asJava)
      table.setTableType(tableType.name)

      table.putToParameters("generated_by", "eel_" + Constants.Version)
      if (tableType == TableType.EXTERNAL_TABLE)
        table.putToParameters("EXTERNAL", "TRUE")
      props.foreach { case (key, value) => table.putToParameters(key, value) }

      client.createTable(table)
      logger.info(s"Table created $databaseName.$tableName")
      true
    } else {
      false
    }
  }

  def createDatabase(name: String, description: String = null, overwrite: Boolean = false) {
    val exists = client.getDatabase(name) != null
    if (exists && overwrite) {
      logger.info(s"Database exists, overwrite=true; dropping database $name")
      client.dropDatabase(name)
    }
    if (overwrite || !exists) {
      val database = new Database(name, description, null, null)
      logger.info(s"Creating database $name")
      client.createDatabase(database)
    }
  }
}
