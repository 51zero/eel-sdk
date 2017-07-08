package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.Constants
import io.eels.component.hive.partition.{PartitionMetaData, PartitionPathStrategy}
import io.eels.schema._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.{Database, FieldSchema, SerDeInfo, StorageDescriptor, Table, Partition => HivePartition}
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType}

import scala.collection.JavaConverters._

// client for operating at a low level on the metastore
// methods in this class will accept/return eel classes, and convert
// the operations into hive specific ones
class HiveOps(val client: IMetaStoreClient) extends Logging {

  /**
    * Returns a map of all partition keys to the distinct values.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  def partitionMap(dbName: String, tableName: String): Map[String, Seq[String]] = {
    partitions(dbName, tableName)
      .flatMap(_.entries)
      .groupBy(_.key)
      .map { case (key, entries) => key -> entries.map(_.value) }
  }

  /**
    * Returns all partition values for a given partition key.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  def partitionValues(dbName: String, tableName: String, key: String): Seq[String] = {
    partitions(dbName, tableName).flatMap(_.entries).find(_.key == key).map(_.value).toSeq.distinct
  }

  // returns the eel field instances which correspond to the partition keys for this table
  def partitionFields(dbName: String, tableName: String): List[Field] = {
    val keys = client.getTable(dbName, tableName).getPartitionKeys.asScala
    keys.map { schema =>
      HiveSchemaFns.fromHiveField(schema).withNullable(false).withPartition(true)
    }.toList
  }

  // returns the eel partitions for this hive table
  def partitions(dbName: String, tableName: String): List[Partition] = {
    val fields = partitionFields(dbName, tableName)
    client.listPartitions(dbName, tableName, Short.MaxValue).asScala.map { it =>
      val entries = it.getValues.asScala.zipWithIndex.map { case (str, int) =>
        PartitionEntry(fields(int).name, str)
      }
      Partition(entries.toList)
    }.toList
  }

  /**
    * Creates a new partition in Hive in the given database.table in the default location, which will be
    * a subdirectory of the table location.
    * The supplied partition path strategy will be used to generate the partition name.
    */
  def createPartition(dbName: String, tableName: String, partition: Partition, strategy: PartitionPathStrategy): Unit = {
    val table = client.getTable(dbName, tableName)
    val location = new Path(table.getSd.getLocation, strategy.name(partition))
    createPartition(dbName, tableName, location, partition.values)
  }

  /**
    * Creates a new partition in Hive in the given database.table. The location of the partition must be
    * specified. If you want to use the default location then use the other variant that doesn't require the
    * location path. The values for the serialization formats are taken from the values for the table.
    */
  def createPartition(dbName: String, tableName: String, location: Path, values: Seq[String]): Unit = {
    logger.info(s"Creating partition ${values.mkString(",")} on $dbName.$tableName at location=$location")

    // we fetch the table so we can copy the serde/format values from the table. It makes no sense
    // to store a partition with different serialization formats to other partitions.
    val table = client.getTable(dbName, tableName)
    val sd = new StorageDescriptor(table.getSd)
    sd.setLocation(location.toString())

    // the hive partition requires the values of the entries
    val hivePartition = new HivePartition(
      values.asJava,
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
    *
    * In Hive, a partition is a set
    */
  def hivePartitions(dbName: String, tableName: String): List[HivePartition] = client.listPartitions(dbName, tableName, Short.MaxValue).asScala.toList

  // returns the partition meta datas for this table
  def partitionsMetaData(dbName: String, tableName: String): Seq[PartitionMetaData] = {
    val keys = client.getTable(dbName, tableName).getPartitionKeys.asScala
    client.listPartitions(dbName, tableName, Short.MaxValue).asScala.map { it =>
      val partition = Partition(keys.zip(it.getValues.asScala).map { case (key, value) => PartitionEntry(key.getName, value) })
      PartitionMetaData(
        new Path(it.getSd.getLocation),
        it.getSd.getLocation,
        it.getSd.getInputFormat,
        it.getSd.getOutputFormat,
        it.getCreateTime,
        it.getLastAccessTime,
        partition
      )
    }
  }
  def createTimeAsInt(): Int = (System.currentTimeMillis() / 1000).toInt

  /**
    * Returns the hive FieldSchema's for partition columns.
    * Hive calls these "partition partitionKeys"
    */
  def partitionFieldSchemas(dbName: String, tableName: String): List[FieldSchema] = client.getTable(dbName, tableName).getPartitionKeys.asScala.toList

  def partitionKeys(dbName: String, tableName: String): List[String] = partitionFieldSchemas(dbName, tableName).map(_.getName)

  def tableExists(databaseName: String, tableName: String): Boolean = client.tableExists(databaseName, tableName)

  def tableFormat(dbName: String, tableName: String): String = client.getTable(dbName, tableName).getSd.getInputFormat

  def location(dbName: String, tableName: String): String = client.getTable(dbName, tableName).getSd.getLocation

  def tablePath(dbName: String, tableName: String): Path = new Path(location(dbName, tableName))

  // Returns the eel schema for the hive dbName:tableName
  def schema(dbName: String, tableName: String): StructType = {
    val table = client.getTable(dbName, tableName)

    // hive columns are always nullable, and hive partitions are never nullable so we can set
    // the nullable fields appropriately
    val columns = table.getSd.getCols.asScala.map { it => HiveSchemaFns.fromHiveField(it) }
    val partitions = table.getPartitionKeys.asScala
      .map { it => HiveSchemaFns.fromHiveField(it).withNullable(false) }
      .map(_.withPartition(true))

    val fields = columns ++ partitions
    StructType(fields.toList)
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

  // returns true if the given partition exists for the given database.table
  def partitionExists(dbName: String,
                      tableName: String,
                      partition: Partition): Boolean = {
    logger.debug(s"Checking if partition exists '${partition.entries.mkString(",")}'")
    import scala.collection.JavaConverters._

    try {
      // when checking if a partition exists, remember the partition path might not actually be
      // the standard hive partition string, so instead pass in the vals which will always work
      client.getPartition(dbName, tableName, partition.values.asJava) != null
    } catch {
      case t: Throwable => false
    }
  }

  // creates (if not existing) the given partition, using the supplied partition path strategy,
  // to determine the format of the path
  def createPartitionIfNotExists(dbName: String,
                                 tableName: String,
                                 partition: Partition,
                                 partitionPathStrategy: PartitionPathStrategy): Unit = {
    logger.debug(s"Ensuring partition exists '$partition'")

    val exists = try {
      client.getPartition(dbName, tableName, partition.values.asJava) != null
    } catch {
      // exception is thrown if the partition doesn't exist, quickest way to find out if it exists
      case _: Throwable => false
    }

    if (!exists) {
      createPartition(dbName, tableName, partition, partitionPathStrategy)
    }
  }

  def createTable(databaseName: String,
                  tableName: String,
                  schema: StructType,
                  partitionKeys: Seq[String],
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
        format.serde,
        Map("serialization.format" -> "1").asJava
      ))
      sd.setInputFormat(format.inputFormat)
      sd.setOutputFormat(format.outputFormat)
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
