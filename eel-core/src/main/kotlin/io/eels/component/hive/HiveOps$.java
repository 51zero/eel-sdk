object HiveOps extends StrictLogging {

  def partitions(dbName: String, tableName: String)
                (implicit client: IMetaStoreClient): List[Partition] = {
    client.listPartitionNames(dbName, tableName, Short.MaxValue).asScala.map(Partition.apply).toList
  }

  /**
    * Returns a map of all partition keys to their values.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  def partitionMap(dbName: String, tableName: String)
                  (implicit client: IMetaStoreClient): Map[String, Seq[String]] = {
    client.listPartitionNames(dbName, tableName, Short.MaxValue).asScala
      .flatMap(p => Partition(p).parts)
      .groupBy(_.key)
      .map { case (key, values) => key -> values.map(_.value) }
  }

  /**
    * Returns all partition values for the given partition keys.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  def partitionValues(dbName: String, tableName: String, keys: Seq[String])
                     (implicit client: IMetaStoreClient): List[Seq[String]] = {
    partitionMap(dbName, tableName).collect { case (key, values) if keys contains key => values }.toList
  }

  /**
    * Returns all partition values for a given partition key.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  def partitionValues(dbName: String, tableName: String, key: String)
                     (implicit client: IMetaStoreClient): Seq[String] = {
    partitionMap(dbName, tableName).getOrElse(key, Nil)
  }

  /**
    * Creates a new partition in Hive in the given database:table in the default location, which will be the
    * partition key values as a subdirectory of the table location. The values for the serialzation formats are
    * taken from the values for the table.
    */
  def createPartition(dbName: String, tableName: String, partition: Partition)
                     (implicit client: IMetaStoreClient): Unit = {
    val table = client.getTable(dbName, tableName)
    val location = new Path(table.getSd.getLocation, partition.name)
    createPartition(dbName, tableName, partition, location)
  }

  /**
    * Creates a new partition in Hive in the given database:table. The location of the partition must be
    * specified. If you want to use the default location then use the other variant that doesn't require the
    * location path. The values for the serialzation formats are taken from the values for the table.
    */
  def createPartition(dbName: String, tableName: String, partition: Partition, location: Path)
                     (implicit client: IMetaStoreClient): Unit = {

    // we fetch the table so we can copy the serde/format values from the table. It makes no sense
    // to store a partition with different serialization formats to other partitions.
    val table = client.getTable(dbName, tableName)
    val sd = new StorageDescriptor(table.getSd)
    sd.setLocation(location.toString)

    val newPartition = new HivePartition(
      partition.values.asJava, // the hive partition values are the actual values of the partition parts
      dbName,
      tableName,
      createTimeAsInt,
      0,
      sd,
      new util.HashMap
    )

    client.add_partition(newPartition)
  }

  def hivePartitions(dbName: String, tableName: String)(implicit client: IMetaStoreClient): List[HivePartition] = {
    client.listPartitions(dbName, tableName, Short.MaxValue).asScala.toList
  }

  def createTimeAsInt: Int = (System.currentTimeMillis / 1000).toInt

  def partitionKeys(dbName: String, tableName: String)(implicit client: IMetaStoreClient): List[FieldSchema] = {
    client.getTable(dbName, tableName).getPartitionKeys.asScala.toList
  }

  def partitionKeyNames(dbName: String, tableName: String)(implicit client: IMetaStoreClient): List[String] = {
    partitionKeys(dbName, tableName).map(_.getName)
  }

  def tableExists(databaseName: String, tableName: String)(implicit client: IMetaStoreClient): Boolean = {
    client.tableExists(databaseName, tableName)
  }

  def tableFormat(dbName: String, tableName: String)(implicit client: IMetaStoreClient): String = {
    client.getTable(dbName, tableName).getSd.getInputFormat
  }

  def location(dbName: String, tableName: String)(implicit client: IMetaStoreClient): String = {
    client.getTable(dbName, tableName).getSd.getLocation
  }

  def tablePath(dbName: String, tableName: String)(implicit client: IMetaStoreClient): Path = {
    new Path(location(dbName, tableName))
  }

  def partitionPath(dbName: String, tableName: String, parts: Seq[PartitionPart])
                   (implicit client: IMetaStoreClient): Path = {
    partitionPath(dbName, tableName, parts, tablePath(dbName, tableName))
  }

  def partitionPath(dbName: String, tableName: String, parts: Seq[PartitionPart], tablePath: Path): Path = {
    new Path(partitionPathString(dbName, tableName, parts, tablePath))
  }

  def partitionPathString(dbName: String, tableName: String, parts: Seq[PartitionPart], tablePath: Path): String = {
    tablePath.toString + "/" + parts.map(_.unquoted).mkString("/")
  }

  // Returns the eel schema for the hive db:table
  def schema(dbName: String, tableName: String)(implicit client: IMetaStoreClient): Schema = {
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
  def addColumn(dbName: String, tableName: String, column: Column)
               (implicit client: IMetaStoreClient): Unit = {
    val table = client.getTable(dbName, tableName)
    val sd = table.getSd
    sd.addToCols(HiveSchemaFns.toHiveField(column))
    client.alter_table(dbName, tableName, table)
  }

  // creates (if not existing) the partition for the given partition parts
  def partitionExists(dbName: String,
                      tableName: String,
                      parts: Seq[PartitionPart])
                     (implicit client: IMetaStoreClient): Boolean = {
    val partitionName = parts.map(_.unquoted).mkString("/")
    logger.debug(s"Checking if partition exists '$partitionName'")
    try {
      client.getPartition(dbName, tableName, partitionName) != null
    } catch {
      case _: Throwable => false
    }
  }

  def applySpec(spec: HiveSpec, overwrite: Boolean)(implicit client: IMetaStoreClient): Unit = {
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
  def createPartitionIfNotExists(dbName: String,
                                 tableName: String,
                                 parts: Seq[PartitionPart])
                                (implicit client: IMetaStoreClient): Unit = {
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

  def createTable(databaseName: String,
                  tableName: String,
                  schema: Schema,
                  partitionKeys: List[String] = Nil,
                  format: HiveFormat = HiveFormat.Text,
                  props: Map[String, String] = Map.empty,
                  tableType: TableType = TableType.MANAGED_TABLE,
                  location: Option[String] = None,
                  overwrite: Boolean = false)
                 (implicit client: IMetaStoreClient): Boolean = {
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