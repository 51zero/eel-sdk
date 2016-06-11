package io.eels.component.hive

import io.eels.util.Logging
import io.eels.Partition
import io.eels.PartitionPart
import io.eels.component.hive.HiveFormat
import io.eels.component.hive.HiveSpecFn
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.StorageDescriptor

class HiveOps(val client: IMetaStoreClient) : Logging {

  fun partitions(dbName: String, tableName: String): List<Partition> =
      client.listPartitionNames(dbName, tableName, Short.MAX_VALUE).map(Partition.apply).toList

  /**
    * Returns a map of all partition keys to their values.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  fun partitionMap(dbName: String, tableName: String): Map<String, List<String>> {
    client.listPartitionNames(dbName, tableName, Short.MAX_VALUE)
        .flatMap { Partition(it).parts }
        .groupBy { it.key }
        .map { key, values -> key -> values.map{ it.value } }
  }

  /**
   * Returns all partition values for the given partition keys.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  fun partitionValues(dbName: String, tableName: String, keys: List<String>): List<List<String>> {
    partitionMap(dbName, tableName).collect { case (key, values) if keys contains key => values }.toList
  }

  /**
    * Returns all partition values for a given partition key.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  fun partitionValues(dbName: String, tableName: String, key: String, ): List<String> {
    partitionMap(dbName, tableName).getOrElse(key, Nil)
  }

  /**
    * Creates a new partition in Hive in the given database:table in the default location, which will be the
    * partition key values as a subdirectory of the table location. The values for the serialzation formats are
    * taken from the values for the table.
    */
  fun createPartition(dbName: String, tableName: String, partition: Partition, ): Unit {
    val table = client.getTable(dbName, tableName)
    val location = new Path(table.getSd.getLocation, partition.name)
    createPartition(dbName, tableName, partition, location)
  }

  /**
    * Creates a new partition in Hive in the given database:table. The location of the partition must be
    * specified. If you want to use the default location then use the other variant that doesn't require the
    * location path. The values for the serialzation formats are taken from the values for the table.
    */
  fun createPartition(dbName: String, tableName: String, partition: Partition, location: Path, ): Unit {

    // we fetch the table so we can copy the serde/format values from the table. It makes no sense
    // to store a partition with different serialization formats to other partitions.
    val table = client.getTable(dbName, tableName)
    val sd = StorageDescriptor(table.sd)
    sd.setLocation(location.toString())

    val newPartition = HivePartition(
        partition.values, // the hive partition values are the actual values of the partition parts
      dbName,
      tableName,
      createTimeAsInt,
      0,
      sd,
      new util.HashMap
    )

    client.add_partition(newPartition)
  }

  fun hivePartitions(dbName: String, tableName: String, ): List<org.apache.hadoop.hive.metastore.api.Partition> =
      client.listPartitions(dbName, tableName, Short.MAX_VALUE)

  fun createTimeAsInt(): Int = (System.currentTimeMillis() / 1000).toInt()

  fun partitionKeys(dbName: String, tableName: String, ): List<FieldSchema> =
      client.getTable(dbName, tableName).partitionKeys

  fun partitionKeyNames(dbName: String, tableName: String): List<String> = partitionKeys(dbName, tableName).map(_.getName)

  fun tableExists(databaseName: String, tableName: String): Boolean = client.tableExists(databaseName, tableName)

  fun tableFormat(dbName: String, tableName: String): String = client.getTable(dbName, tableName).sd.inputFormat

  fun location(dbName: String, tableName: String): String = client.getTable(dbName, tableName).sd.location

  fun tablePath(dbName: String, tableName: String): Path = Path(location(dbName, tableName))


  fun partitionPath(dbName: String, tableName: String, parts: List<PartitionPart>): Path = {
    partitionPath(dbName, tableName, parts, tablePath(dbName, tableName))
  }

  fun partitionPath(dbName: String, tableName: String, parts: Seq[PartitionPart], tablePath: Path): Path
  {
    new Path(partitionPathString(dbName, tableName, parts, tablePath))
  }

  fun partitionPathString(dbName: String, tableName: String, parts: Seq[PartitionPart], tablePath: Path): String
  {
    tablePath.toString + "/" + parts.map(_.unquoted).mkString("/")
  }

  // Returns the eel schema for the hive db:table
  fun schema(dbName: String, tableName: String)( ): Schema =
  {
    val table = client.getTable(dbName, tableName)
    // hive columns are always nullable, and hive partitions are never nullable
    val columns = table.getSd.getCols.asScala.map(HiveSchemaFns.fromHiveField(_, true)) ++
      table.getPartitionKeys.asScala.map(HiveSchemaFns.fromHiveField(_, false))
    Schema(columns.toList)
  }

  /**
    * Adds this column to the hive schema. This is schema evolution.
    * The column must be marked as nullable and cannot have the same name as an existing column.
    */
  fun addColumn(dbName: String, tableName: String, column: Column)
  ( ): Unit =
  {
    val table = client.getTable(dbName, tableName)
    val sd = table.getSd
    sd.addToCols(HiveSchemaFns.toHiveField(column))
    client.alter_table(dbName, tableName, table)
  }

  // creates (if not existing) the partition for the given partition parts
  fun partitionExists(dbName: String,
                      tableName: String,
                      parts: Seq[PartitionPart])
  ( ): Boolean =
  {
    val partitionName = parts.map(_.unquoted).mkString("/")
    logger.debug(s"Checking if partition exists '$partitionName'")
    try {
      client.getPartition(dbName, tableName, partitionName) != null
    } catch {
      case _: Throwable => false
    }
  }

  fun applySpec(spec: HiveSpec, overwrite: Boolean)( ): Unit =
  {
    spec.tables.foreach { table =>
      val schemas = HiveSpecFn.toSchemas(spec)
      createTable(spec.dbName,
        table.tableName,
        schemas(table.tableName),
        table.partitionKeys,
        HiveFormat.fromInputFormat(table.inputFormat),
        Map.empty,
        TableType.MANAGED_TABLE,
        None,
        overwrite
      )
    }
  }

  // creates (if not existing) the partition for the given partition parts
  fun createPartitionIfNotExists(dbName: String,
                                 tableName: String,
                                 parts: Seq[PartitionPart])
  ( ): Unit =
  {
    val partitionName = parts.map(_.unquoted).mkString("/")
    logger.debug(s"Ensuring partition exists '$partitionName'")
    val exists = try {
      client.getPartition(dbName, tableName, partitionName) != null
    } catch {
      case _: Throwable => false
    }

    if (!exists) {

      val path = partitionPath(dbName, tableName, parts)
      logger.debug(s"Creating partition '$partitionName' at $path")

      val partition = Partition(parts.toList)
      createPartition(dbName, tableName, partition)
    }
  }

  fun createTable(databaseName: String,
                  tableName: String,
                  schema: Schema,
                  partitionKeys: List[String] = Nil,
                  format: HiveFormat = HiveFormat.Text,
                  props: Map[String, String] = Map.empty,
                  tableType: TableType = TableType.MANAGED_TABLE,
                  location: Option[String] = None,
                  overwrite: Boolean = false)
  ( ): Boolean =
  {
    require(partitionKeys.forall(schema.contains), s"Schema must define all partition keys ${partitionKeys.mkString(",")}")

    if (overwrite) {
      logger.debug("Removing table if exists (overwrite mode = true)")
      if (tableExists(databaseName, tableName))
        client.dropTable(databaseName, tableName, true, true, true)
    }

    if (!tableExists(databaseName, tableName)) {
      logger.info(s"Creating table $databaseName.$tableName with partitionKeys=${partitionKeys.mkString(",")}")

      val lowerPartitionKeys = partitionKeys.map(_.toLowerCase)
      val lowerColumns = schema.columns.map(_.toLowerCase)

      val sd = new StorageDescriptor()
      val fields = lowerColumns.filterNot(lowerPartitionKeys contains _.name).map(HiveSchemaFns.toHiveField).asJava
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
      table.setPartitionKeys(lowerPartitionKeys.map(new FieldSchema(_, "string", null)).asJava)
      table.setTableType(tableType.name)

      table.putToParameters("generated_by", "eel_" + Constants.EelVersion)
      if (tableType == TableType.EXTERNAL_TABLE)
        table.putToParameters("EXTERNAL", "TRUE")
      props.foreach { case (key, value) =>
        table.putToParameters(key, value)
      }

      client.createTable(table)
      logger.info(s"Table created $databaseName.$tableName")
      true
    } else {
      false
    }
  }
}
