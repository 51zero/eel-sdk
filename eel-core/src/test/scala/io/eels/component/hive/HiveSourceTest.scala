package io.eels.component.hive

import java.util.UUID

import io.eels.{Frame, FrameSchema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class HiveSourceTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  val table = getClass.getSimpleName.toLowerCase
  val database = "sam"

  val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()
  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)

  val schema = FrameSchema("a", "b", "c", "d")
  val rows = List.fill(100)(List(UUID.randomUUID.toString, Random.nextInt, Random.nextDouble, Random.nextBoolean))
  val frame = Frame(schema, rows)

  HiveOps.createTable(
    database,
    table,
    frame.schema,
    format = HiveFormat.Parquet,
    overwrite = true
  )

  frame.to(HiveSink(database, table))

  "HiveSource.schema" should {
    "only include projected columns" ignore {
      val source = HiveSource(database, table).withColumns("a", "c")
      source.schema.columnNames shouldBe List("a", "c")
    }
    "include all columns for *" ignore {
      val source = HiveSource(database, table)
      source.schema.columnNames shouldBe List("a", "b", "c", "d")
    }
  }

  "HiveSource.buffer" should {
    "only include projected columns" ignore {
      val source = HiveSource(database, table).withColumns("a", "c")
      source.toSeq.head.size shouldBe 2
    }
    "include all columns for *" ignore {
      val source = HiveSource(database, table)
      source.toSeq.head.size shouldBe 4
    }
  }
}
