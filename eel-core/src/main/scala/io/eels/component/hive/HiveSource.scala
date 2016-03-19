package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.io.Using
import io.eels._
import io.eels.component.parquet.ParquetLogMute
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

object HiveSource {
  def apply(dbName: String, tableName: String)
           (implicit fs: FileSystem, client: IMetaStoreClient): HiveSource = new HiveSource(dbName, tableName)
}

case class HiveSource(val dbName: String,
                      val tableName: String,
                      private val partitionExprs: List[PartitionConstraint] = Nil,
                      private val columnNames: Seq[String] = Nil,
                      private val predicate: Option[Predicate] = None)
                     (implicit fs: FileSystem, client: IMetaStoreClient)
  extends Source
    with Logging
    with Using {
  ParquetLogMute()

  def withColumns(columns: Seq[String]): HiveSource = {
    require(columns.nonEmpty)
    copy(columnNames = columns)
  }

  def create(schema: org.apache.avro.Schema): Unit = {

  }

  def withColumns(first: String, rest: String*): HiveSource = withColumns(first +: rest)

  def withPredicate(predicate: Predicate): HiveSource = copy(predicate = Some(predicate))

  def withPartitionConstraint(name: String, value: String): HiveSource = withPartitionConstraint(PartitionEquals(name, value))

  def withPartitionConstraint(expr: PartitionConstraint): HiveSource = {
    copy(partitionExprs = partitionExprs :+ expr)
  }

  def partitions: List[Partition] = HiveOps.partitions(dbName, tableName)

  /**
    * Returns all partition values for a given partition key.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  def partitionValues(key: String): Seq[String] = {
    HiveOps.partitionValues(dbName, tableName, key)(client)
  }

  /**
    * Returns all partition values for the given partition keys.
    * This operation is optimized, in that it does not need to scan files, but can retrieve the information
    * directly from the hive metastore.
    */
  def partitionValues(keys: Seq[String]): Seq[Seq[String]] = {
    HiveOps.partitionValues(dbName, tableName, keys)(client)
  }

  def partitionMap: Map[String, Seq[String]] = HiveOps.partitionMap(dbName, tableName)(client)

  def partitionMap(keys: Seq[String]): Map[String, Seq[String]] = keys.zip(partitionValues(keys)).toMap

  /**
    * The returned schema should take into account:
    *
    * 1) Any projection. If a projection is set, then it should return the schema in the same order
    * as the projection. If no projection is set then the schema should be driven from the hive metastore.
    *
    * 2) Any partitions set. These should be included in the schema columns.
    */
  override lazy val schema: Schema = {

    // if no columnNames were specified, then we will return the schema as is from the hive database,
    // otherwise we will keep only the specified columnNames
    if (columnNames.isEmpty) metastoreSchema
    else {
      // remember hive is always lower case, so when comparing requested columnNames with
      // hive field names we need to use lower case everything. And we need to maintain
      // the order of the schema with respect to the projection
      val columns = columnNames.map { columnName =>
        metastoreSchema.columns.find(_.name equalsIgnoreCase columnName)
          .getOrElse(sys.error(s"Requested column $columnName does not exist in the hive source"))
      }
      Schema(columns.toList)
    }
  }

  // returns the full underlying schema from the metastore including partition keys
  lazy val metastoreSchema: Schema = HiveOps.schema(dbName, tableName)

  // returns just the partition keys in definition order
  lazy val partitionKeys: List[String] = HiveOps.partitionKeyNames(dbName, tableName)

  def spec: HiveSpec = HiveSpecFn.toHiveSpec(dbName, tableName)

  override def parts: Seq[Part] = {

    val table = client.getTable(dbName, tableName)
    val dialect = HiveDialect(table)
    val paths = HiveFilesFn(table, partitionExprs)
    logger.debug(s"Found ${paths.size} visible hive files from all locations for $dbName:$tableName")

    // if we requested only partition columns, then we can get this information by scanning the file system
    // to see which partitions have been created. Those files must indicate partitions that have data.
    if (schema.columnNames forall partitionKeys.contains) {
      logger.debug("Requested columns are all partitions; reading directly from metastore")
      // we pass in the columns so we can order the results to keep them aligned with the given withColumns ordering
      Seq(new HivePartitionPart(dbName, tableName, schema.columnNames, partitionKeys, metastoreSchema, predicate, dialect))
    } else {
      paths.map { case (file, partition) =>
        new HiveFilePart(dialect, file, partition, metastoreSchema, schema, predicate, partitionKeys)
      }
    }
  }
}