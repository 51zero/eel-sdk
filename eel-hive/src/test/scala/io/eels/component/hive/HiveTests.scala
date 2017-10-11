package io.eels.component.hive

import com.sksamuel.exts.Logging
import org.scalatest.{Matchers, WordSpec}

abstract class HiveTests extends WordSpec with Matchers with Logging {

//  val hdfs = new MiniDFSCluster.Builder(new Configuration).build()
//  hdfs.waitClusterUp()
//  logger.info("Mini Cluster is UP")
//
//  val hiveLocalMetaStore = new HiveLocalMetaStore.Builder()
//    .setHiveMetastoreHostname("localhost")
//    .setHiveMetastorePort(12347)
//    .setHiveMetastoreDerbyDbDir("metastore_db")
//    .setHiveScratchDir("hive_scratch_dir")
//    .setHiveWarehouseDir("warehouse_dir")
//    .setHiveConf(new HiveConf)
//    .build()
//
//  hiveLocalMetaStore.start()
//  logger.info("Hive metastore is UP")
//
//  val hdfsRoot = new Path(s"hdfs://${hdfs.getNameNode.getHostAndPort}")
//
//  implicit val fs = hdfs.getFileSystem
//  implicit val metastore = new HiveMetaStoreClient(hiveLocalMetaStore.getHiveConf)
}
