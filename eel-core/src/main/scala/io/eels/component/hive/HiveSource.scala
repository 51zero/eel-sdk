package io.eels.component.hive

import com.sksamuel.scalax.Logging
import com.sksamuel.scalax.io.Using
import io.eels._
import io.eels.component.parquet.ParquetLogMute
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.collection.JavaConverters._

object HiveSource {
  def apply(dbName: String, tableName: String)
           (implicit fs: FileSystem, client: IMetaStoreClient): HiveSource = new HiveSource(dbName, tableName)
}

case class HiveSource(private val dbName: String,
                      private val tableName: String,
                      private val partitionExprs: List[PartitionConstraint] = Nil,
                      private val columnNames: Seq[String] = Nil)
                     (implicit fs: FileSystem, client: IMetaStoreClient)
  extends Source
    with Logging
    with Using {
  ParquetLogMute()

  def withColumns(columns: Seq[String]): HiveSource = {
    require(columns.nonEmpty)
    copy(columnNames = columns)
  }

  def withColumns(first: String, rest: String*): HiveSource = withColumns(first +: rest)

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
    val paths = HiveFilesFn(table, partitionExprs)
    logger.debug(s"Found ${paths.size} visible hive files from all locations for $dbName:$tableName")

    // if we requested only partition columns, then we can get this information by scanning the file system
    // to see which partitions have been created. Those files must indicate partitions that have data.
    if (schema.columnNames forall partitionKeys.contains) {
      logger.debug("Requested columns are all partitions; reading directly from metastore")
      // we pass in the columns so we can order them to the requested withColumns ordering
      Seq(new HivePartitionPart(dbName, tableName, schema.columnNames, partitionKeys))
    } else {
      val dialect = HiveDialect(table)
      paths.map { case (file, partition) =>
        new HiveFilePart(dialect, file, partition, metastoreSchema, schema, partitionKeys)
      }
    }
  }
}

class HivePartitionPart(dbName: String, tableName: String, columnNames: Seq[String], partitionKeys: Seq[String])
                       (implicit fs: FileSystem, client: IMetaStoreClient) extends Part {

  override def reader: SourceReader = {

    val values = client.listPartitions(dbName, tableName, Short.MaxValue).asScala.filter { partition =>
      fs.exists(new Path(partition.getSd.getLocation))
    } map { partition =>
      val map = partitionKeys.zip(partition.getValues.asScala).toMap
      columnNames.map(map(_)).toVector
    }

    new SourceReader {
      override def close(): Unit = ()
      override def iterator: Iterator[InternalRow] = values.iterator
    }
  }
}

class HiveFilePart(dialect: HiveDialect,
                   file: LocatedFileStatus,
                   partition: Partition,
                   metastoreSchema: Schema,
                   schema: Schema,
                   partitionKeys: Seq[String])
                  (implicit fs: FileSystem) extends Part {

  override def reader: SourceReader = {

    // the schema we send to the reader must have any partitions removed, because those columns won't exist
    // in the data files. This is because partitions are not written and instead inferred from the hive meta store.
    val dataSchema = Schema(schema.columns.filterNot(partitionKeys contains _.name))
    val reader = dialect.reader(file.getPath, metastoreSchema, dataSchema)

    new SourceReader {
      override def close(): Unit = reader.close()

      // when we read a row back from the dialect reader, we must repopulate any partition columns requested.
      // Again because those values are not stored in hive, but inferred from the meta store
      override def iterator: Iterator[InternalRow] = reader.iterator.map { row =>
        val map = RowUtils.toMap(dataSchema, row)
        schema.columnNames.map { columnName =>
          map.getOrElse(columnName, partition.get(columnName).orNull)
        }
      }
    }
  }
}
