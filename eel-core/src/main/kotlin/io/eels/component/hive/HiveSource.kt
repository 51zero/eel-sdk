package io.eels.component.hive

import io.eels.schema.PartitionConstraint
import io.eels.schema.PartitionEquals
import io.eels.Source
import io.eels.component.Part
import io.eels.component.Predicate
import io.eels.component.Using
import io.eels.component.parquet.ParquetLogMute
import io.eels.schema.Field
import io.eels.schema.Schema
import io.eels.util.Logging
import io.eels.util.Option
import org.apache.hadoop.hive.metastore.IMetaStoreClient

data class HiveSource(val dbName: String,
                      val tableName: String,
                      private val partitionExprs: List<PartitionConstraint>,
                      private val columnNames: List<String>,
                      private val predicate: Option<Predicate>,
                      private val client: IMetaStoreClient) : Source, Logging, Using {
  init {
    ParquetLogMute()
  }

  val ops = HiveOps(client)

  fun withColumns(columns: List<String>): HiveSource {
    require(columns.isNotEmpty())
    return copy(columnNames = columns)
  }

  fun withColumns(vararg columns: String): HiveSource = withColumns(columns.asList())

  fun withPredicate(predicate: Predicate): HiveSource = copy(predicate = Option.Some(predicate))

  fun withPartitionConstraint(name: String, value: String): HiveSource = withPartitionConstraint(PartitionEquals(name, value))

  fun withPartitionConstraint(expr: PartitionConstraint): HiveSource {
    return copy(partitionExprs = partitionExprs.plus(expr))
  }

//  /**
//   * Returns all partition values for a given partition key.
//   * This operation is optimized, in that it does not need to scan files, but can retrieve the information
//   * directly from the hive metastore.
//   */
//  fun partitionValues(key: String): List<String> =
//      HiveOps.partitionValues(dbName, tableName, key)(client)
//

//
//  fun partitionMap(): Map<String, List<String>> = HiveOps.partitionMap(dbName, tableName)(client)
//
//  fun partitionMap(keys: List<String>): Map<String, List<String>> = keys.zip(partitionValues(keys))

  /**
   * The returned schema should take into account:
   *
   * 1) Any projection. If a projection is set, then it should return the schema in the same order
   * as the projection. If no projection is set then the schema should be driven from the hive metastore.
   *
   * 2) Any partitions set. These should be included in the schema columns.
   */
  override fun schema(): Schema {

    // if no columnNames were specified, then we will return the schema as is from the hive database,
    // otherwise we will keep only the specified columnNames
    val schema = if (columnNames.isEmpty()) metastoreSchema
    else {
      // remember hive is always lower case, so when comparing requested columnNames with
      // hive field names we need to use lower case everything. And we need to maintain
      // the order of the schema with respect to the projection
      //val columns = columnNames.map {
      //  metastoreSchema.columns.find { it.name equalsIgnoreCase it }
      //       .getOrElse(sys.error("Requested column $columnName does not exist in the hive source"))
      // }
      val columns = emptyList<Field>()
      Schema(columns)
    }

    return schema
  }

  // returns the full underlying schema from the metastore including partition keys
  val metastoreSchema: Schema = ops.schema(dbName, tableName)

  // returns just the partition keys in funinition order
  val partitionKeys: List<String> = ops.partitionKeyNames(dbName, tableName)

  //fun spec(): HiveSpec = HiveSpecFn.toHiveSpec(dbName, tableName)

  override fun parts(): List<Part> = emptyList()

//    val table = client.getTable(dbName, tableName)
//    val dialect = io.eels.component.hive.HiveDialect(table)
//    val paths = HiveFilesFn(table, partitionExprs)
//    logger.debug("Found ${paths.size} visible hive files from all locations for $dbName:$tableName")
//
//    // if we requested only partition columns, then we can get this information by scanning the file system
//    // to see which partitions have been created. Those files must indicate partitions that have data.
//    if (schema.columnNames forall partitionKeys.contains) {
//      logger.debug("Requested columns are all partitions; reading directly from metastore")
//      // we pass in the columns so we can order the results to keep them aligned with the given withColumns ordering
//      Seq(new HivePartitionPart(dbName, tableName, schema.columnNames, partitionKeys, metastoreSchema, predicate, dialect))
//    } else {
//      paths.map {
//        new HiveFilePart(dialect, file, partition, metastoreSchema, schema, predicate, partitionKeys)
//      }
//    }
//  }
}