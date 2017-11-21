package io.eels.component.hive

import java.nio.file.Paths

import com.sksamuel.exts.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class HiveTableFilesFnTest extends FlatSpec with Matchers with Logging with MockitoSugar {

  System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA)
  val clusterPath = Paths.get("miniclusters", "cluster")
  val conf = new Configuration()
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, clusterPath.toAbsolutePath.toString)
  val cluster = new MiniDFSCluster.Builder(conf).build()
  implicit val fs = cluster.getFileSystem

  "HiveTableFilesFn" should "detect all files in root when no partitions" in {

    implicit val client = mock[IMetaStoreClient]
    org.mockito.Mockito.when(client.getTable("default", "mytable")).thenReturn(new Table)

    val root = new Path("tab1")
    fs.mkdirs(root)

    // table scanner will skip 0 length files
    val a = fs.create(new Path(root, "a"))
    a.write(1)
    a.close()

    val b = fs.create(new Path(root, "b"))
    b.write(1)
    b.close()

    HiveTableFilesFn("default", "mytable", fs.resolvePath(root), Nil).values.flatten.map(_.getPath.getName).toSet shouldBe Set("a", "b")
  }

  it should "ignore hidden files in root when no partitions" in {
    implicit val client = mock[IMetaStoreClient]
    org.mockito.Mockito.when(client.getTable("default", "mytable")).thenReturn(new Table)

    val root = new Path("tab2")
    fs.mkdirs(root)

    // table scanner will skip 0 length files
    val a = fs.create(new Path(root, "a"))
    a.write(1)
    a.close()

    val b = fs.create(new Path(root, "_b"))
    b.write(1)
    b.close()

    HiveTableFilesFn("default", "mytable", fs.resolvePath(root), Nil).values.flatten.map(_.getPath.getName).toSet shouldBe Set("a")
  }
}
