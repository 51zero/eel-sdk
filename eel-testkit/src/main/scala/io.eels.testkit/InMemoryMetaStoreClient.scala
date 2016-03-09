package io.eels.testkit

import java.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.{ObjectPair, ValidTxnList}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter
import org.apache.hadoop.hive.metastore.api.{Database, _}
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy

import scala.collection.JavaConverters._

/**
  * @param home the location where databases will be created
  * @param fs   a filesystem that can be used for file operations
  */
class InMemoryMetaStoreClient(home: String, fs: FileSystem) extends IMetaStoreClient {

  private val databases = scala.collection.mutable.Map.empty[String, Database]
  private val tables = scala.collection.mutable.Map.empty[String, Seq[Table]]
  private val partitions = scala.collection.mutable.Map.empty[String, Seq[Partition]]

  // databases
  override def getDatabase(databaseName: String): Database = databases.getOrElse(databaseName, throw new NoSuchObjectException)
  override def alterDatabase(name: String, db: Database): Unit = ???
  override def dropDatabase(name: String): Unit = databases.remove(name)
  override def dropDatabase(name: String, deleteData: Boolean, ignoreUnknownDb: Boolean): Unit = databases.remove(name)
  override def dropDatabase(name: String, deleteData: Boolean, ignoreUnknownDb: Boolean, cascade: Boolean): Unit = databases.remove(name)

  override def createDatabase(db: Database): Unit = {
    val location = new Path(home, db.getName)
    fs.mkdirs(location)
    databases.put(db.getName, db)
  }

  override def getDatabases(databasePattern: String): util.List[String] = databases.keysIterator.toList.filter(_.matches(databasePattern.replace("*", ".*?"))).asJava
  override def getAllDatabases: util.List[String] = databases.keysIterator.toList.asJava

  // tables
  override def createTable(tbl: Table): Unit = {
    if (tableExists(tbl.getDbName, tbl.getTableName))
      throw new IllegalStateException(tbl.getTableName + "already exists")

    if (tbl.getSd.getLocation == null) {
      tbl.getSd.setLocation(new Path(new Path(home, tbl.getDbName), tbl.getTableName).toString)
    }
    val path = new Path(tbl.getSd.getLocation)
    fs.mkdirs(path)
    tables.put(tbl.getDbName, tables.getOrElse(tbl.getDbName, Nil) :+ tbl)
  }

  override def dropTable(dbName: String, tableName: String, deleteData: Boolean, ignoreUnknownTab: Boolean): Unit = {
    if (deleteData) {
      tables.getOrElse(dbName, Nil).find(_.getTableName == tableName) match {
        case Some(table) =>
          val path = new Path(table.getSd.getLocation)
          fs.delete(path, true)
        case _ =>
      }
    }
    tables.put(dbName, tables.getOrElse(dbName, Nil).filterNot(_.getTableName == tableName))
  }

  override def dropTable(dbName: String, tableName: String, deleteData: Boolean, ignoreUnknownTab: Boolean, ifPurge: Boolean): Unit = {
    dropTable(dbName, tableName, deleteData, ignoreUnknownTab)
  }

  override def dropTable(tableName: String, deleteData: Boolean): Unit = sys.error("use dropTable(dbName, tableName)")
  override def dropTable(dbName: String, tableName: String): Unit = dropTable(dbName, tableName, false, false)

  override def listTableNamesByFilter(dbName: String, filter: String, maxTables: Short): util.List[String] = ???
  override def getAllTables(dbName: String): util.List[String] = ???
  override def getTableObjectsByName(dbName: String, tableNames: util.List[String]): util.List[Table] = ???
  override def getTables(dbName: String, tablePattern: String): util.List[String] = ???

  override def alter_table(defaultDatabaseName: String, tblName: String, table: Table): Unit = ???
  override def alter_table(defaultDatabaseName: String, tblName: String, table: Table, cascade: Boolean): Unit = ???

  override def getTable(tableName: String): Table = sys.error("Use getTable(dbName, tableName)")
  override def getTable(dbName: String, tableName: String): Table = tables(dbName).find(_.getTableName == tableName).orNull
  override def tableExists(dbName: String, tableName: String): Boolean = {
    tables.getOrElse(dbName, Nil).exists(_.getTableName == tableName)
  }
  override def tableExists(tableName: String): Boolean = sys.error("Use tableExists(dbName, tableName)")

  // permissions
  override def get_privilege_set(hiveObject: HiveObjectRef, user_name: String, group_names: util.List[String]): PrincipalPrivilegeSet = ???
  override def get_role_grants_for_principal(getRolePrincReq: GetRoleGrantsForPrincipalRequest): GetRoleGrantsForPrincipalResponse = ???
  override def listRoleNames(): util.List[String] = ???
  override def list_privileges(principal_name: String, principal_type: PrincipalType, hiveObject: HiveObjectRef): util.List[HiveObjectPrivilege] = ???
  override def drop_role(role_name: String): Boolean = ???
  override def get_principals_in_role(getPrincRoleReq: GetPrincipalsInRoleRequest): GetPrincipalsInRoleResponse = ???
  override def grant_role(role_name: String, user_name: String, principalType: PrincipalType, grantor: String, grantorType: PrincipalType, grantOption: Boolean): Boolean = ???
  override def checkLock(lockid: Long): LockResponse = ???
  override def list_roles(principalName: String, principalType: PrincipalType): util.List[Role] = ???
  override def getDelegationToken(owner: String, renewerKerberosPrincipalName: String): String = ???
  override def create_role(role: Role): Boolean = ???
  override def grant_privileges(privileges: PrivilegeBag): Boolean = ???
  override def revoke_role(role_name: String, user_name: String, principalType: PrincipalType, grantOption: Boolean): Boolean = ???
  override def revoke_privileges(privileges: PrivilegeBag, grantOption: Boolean): Boolean = ???

  // partitions
  override def getPartition(dbName: String, tblName: String, name: String): Partition = {
    val key = dbName + ":" + tblName
    val partitionKeys = getTable(dbName, tblName).getPartitionKeys.asScala.map(_.getName)
    def dirname(values: Seq[String]): String = partitionKeys.zip(values).map { case (k, v) => s"$k=$v" }.mkString("/")
    partitions.getOrElse(key, Nil).find(part => dirname(part.getValues.asScala) == name).orNull
  }

  override def getPartition(dbName: String, tblName: String, partVals: util.List[String]): Partition = {
    val key = dbName + ":" + tblName
    partitions.getOrElse(key, Nil).find(_.getValues.asScala.toList == partVals.asScala.toList).orNull
  }

  override def add_partition(partition: Partition): Partition = {
    val key = partition.getDbName + ":" + partition.getTableName
    partitions.put(key, partitions.getOrElse(key, Nil) :+ partition)
    partition
  }

  override def getPartitionsByNames(db_name: String, tbl_name: String, part_names: util.List[String]): util.List[Partition] = ???
  override def dropPartition(db_name: String, tbl_name: String, part_vals: util.List[String], deleteData: Boolean): Boolean = ???
  override def dropPartition(db_name: String, tbl_name: String, name: String, deleteData: Boolean): Boolean = ???
  override def dropFunction(dbName: String, funcName: String): Unit = ???
  override def renamePartition(dbname: String, name: String, part_vals: util.List[String], newPart: Partition): Unit = ???
  override def isPartitionMarkedForEvent(db_name: String, tbl_name: String, partKVs: util.Map[String, String], eventType: PartitionEventType): Boolean = ???
  override def listPartitionSpecsByFilter(db_name: String, tbl_name: String, filter: String, max_parts: Int): PartitionSpecProxy = ???
  override def listPartitionsByExpr(db_name: String, tbl_name: String, expr: Array[Byte], default_partition_name: String, max_parts: Short, result: util.List[Partition]): Boolean = ???
  override def listPartitionSpecs(dbName: String, tableName: String, maxParts: Int): PartitionSpecProxy = ???
  override def add_partitions(partitions: util.List[Partition]): Int = ???
  override def add_partitions(partitions: util.List[Partition], ifNotExists: Boolean, needResults: Boolean): util.List[Partition] = ???
  override def appendPartition(tableName: String, dbName: String, partVals: util.List[String]): Partition = ???
  override def appendPartition(tableName: String, dbName: String, name: String): Partition = ???
  override def getPartitionWithAuthInfo(dbName: String, tableName: String, pvals: util.List[String], userName: String, groupNames: util.List[String]): Partition = ???
  override def partitionNameToSpec(name: String): util.Map[String, String] = ???
  override def listPartitionsWithAuthInfo(dbName: String, tableName: String, s: Short, userName: String, groupNames: util.List[String]): util.List[Partition] = ???
  override def listPartitionsWithAuthInfo(dbName: String, tableName: String, partialPvals: util.List[String], s: Short, userName: String, groupNames: util.List[String]): util.List[Partition] = ???
  override def listPartitionsByFilter(db_name: String, tbl_name: String, filter: String, max_parts: Short): util.List[Partition] = ???
  override def listPartitions(db_name: String, tbl_name: String, max_parts: Short): util.List[Partition] = ???
  override def listPartitions(db_name: String, tbl_name: String, part_vals: util.List[String], max_parts: Short): util.List[Partition] = ???
  override def validatePartitionNameCharacters(partVals: util.List[String]): Unit = ???
  override def alter_partitions(dbName: String, tblName: String, newParts: util.List[Partition]): Unit = ???
  override def dropPartitions(dbName: String, tblName: String, partExprs: util.List[ObjectPair[Integer, Array[Byte]]], deleteData: Boolean, ignoreProtection: Boolean, ifExists: Boolean): util.List[Partition] = ???
  override def add_partitions_pspec(partitionSpec: PartitionSpecProxy): Int = ???
  override def partitionNameToVals(name: String): util.List[String] = ???
  override def exchange_partition(partitionSpecs: util.Map[String, String], sourceDb: String, sourceTable: String, destdb: String, destTableName: String): Partition = ???
  override def markPartitionForEvent(db_name: String, tbl_name: String, partKVs: util.Map[String, String], eventType: PartitionEventType): Unit = ???
  override def alter_partition(dbName: String, tblName: String, newPart: Partition): Unit = ???
  override def listPartitionNames(db_name: String, tbl_name: String, max_parts: Short): util.List[String] = ???
  override def listPartitionNames(db_name: String, tbl_name: String, part_vals: util.List[String], max_parts: Short): util.List[String] = ???

  // schemas
  override def getSchema(db: String, tableName: String): util.List[FieldSchema] = ???
  override def getFields(db: String, tableName: String): util.List[FieldSchema] = ???

  // txs
  override def openTxn(user: String): Long = ???
  override def rollbackTxn(txnid: Long): Unit = ???
  override def commitTxn(txnid: Long): Unit = ???
  override def getValidTxns: ValidTxnList = ???
  override def openTxns(user: String, numTxns: Int): OpenTxnsResponse = ???
  override def heartbeat(txnid: Long, lockid: Long): Unit = ???
  override def getValidTxns(currentTxn: Long): ValidTxnList = ???
  override def showTxns(): GetOpenTxnsInfoResponse = ???
  override def heartbeatTxnRange(min: Long, max: Long): HeartbeatTxnRangeResponse = ???
  override def showLocks(): ShowLocksResponse = ???

  // indexes
  override def createIndex(index: Index, indexTable: Table): Unit = ???
  override def listIndexes(db_name: String, tbl_name: String, max: Short): util.List[Index] = ???
  override def listIndexNames(db_name: String, tbl_name: String, max: Short): util.List[String] = ???
  override def getIndex(dbName: String, tblName: String, indexName: String): Index = ???
  override def alter_index(dbName: String, tblName: String, indexName: String, index: Index): Unit = ???
  override def dropIndex(db_name: String, tbl_name: String, name: String, deleteData: Boolean): Boolean = ???

  // statistics
  override def setPartitionColumnStatistics(request: SetPartitionsStatsRequest): Boolean = ???
  override def showCompactions(): ShowCompactResponse = ???
  override def getTableColumnStatistics(dbName: String, tableName: String, colNames: util.List[String]): util.List[ColumnStatisticsObj] = ???
  override def updatePartitionColumnStatistics(statsObj: ColumnStatistics): Boolean = ???
  override def getPartitionColumnStatistics(dbName: String, tableName: String, partNames: util.List[String], colNames: util.List[String]): util.Map[String, util.List[ColumnStatisticsObj]] = ???
  override def deletePartitionColumnStatistics(dbName: String, tableName: String, partName: String, colName: String): Boolean = ???
  override def deleteTableColumnStatistics(dbName: String, tableName: String, colName: String): Boolean = ???
  override def updateTableColumnStatistics(statsObj: ColumnStatistics): Boolean = ???
  override def getAggrColStatsFor(dbName: String, tblName: String, colNames: util.List[String], partName: util.List[String]): AggrStats = ???

  override def unlock(lockid: Long): Unit = ???
  override def getFunction(dbName: String, funcName: String): Function = ???
  override def getCurrentNotificationEventId: CurrentNotificationEventId = ???
  override def getFunctions(dbName: String, pattern: String): util.List[String] = ???
  override def lock(request: LockRequest): LockResponse = ???
  override def renewDelegationToken(tokenStrForm: String): Long = ???
  override def getMetaConf(key: String): String = ???
  override def isCompatibleWith(conf: HiveConf): Boolean = ???
  override def alterFunction(dbName: String, funcName: String, newFunction: Function): Unit = ???
  override def getConfigValue(name: String, defaultValue: String): String = ???
  override def compact(dbname: String, tableName: String, partitionName: String, `type`: CompactionType): Unit = ???
  override def setMetaConf(key: String, value: String): Unit = ???
  override def createFunction(func: Function): Unit = ???
  override def cancelDelegationToken(tokenStrForm: String): Unit = ???
  override def getNextNotification(lastEventId: Long, maxEvents: Int, filter: NotificationFilter): NotificationEventResponse = ???

  // in memory database doesn't need to connect/disconnect
  override def reconnect(): Unit = ()
  override def close(): Unit = ()
}
