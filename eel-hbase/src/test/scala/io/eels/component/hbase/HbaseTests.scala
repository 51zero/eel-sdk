package io.eels.component.hbase

import java.nio.file.Paths
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hdfs.MiniDFSCluster

trait HbaseTests {
  val MINI_CLUSTER_ROOT = "miniclusters"

  def startHBaseCluster(clusterName: String): MiniHBaseCluster = {
    // Setup the underlying HDFS mini cluster for HBASE mini cluster
    System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA)
    val clusterFolder = s"${clusterName}_${UUID.randomUUID().toString}"
    val clusterPath = Paths.get(MINI_CLUSTER_ROOT, clusterFolder)
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterPath.toAbsolutePath.toString)
    val miniDFSCluster = new MiniDFSCluster.Builder(conf).build()

    // Now setup and start the HBASE mini cluster
    val hBaseTestingUtility = new HBaseTestingUtility
    hBaseTestingUtility.setDFSCluster(miniDFSCluster)
    hBaseTestingUtility.startMiniCluster(1, 1)
    val cluster = hBaseTestingUtility.getHBaseCluster
    cluster.waitForActiveAndReadyMaster()
    cluster
  }

}
