package io.eels.component.hive

import com.sksamuel.exts.Logging
import io.eels.Frame
import io.eels.component.hdfs.HdfsSource
import io.eels.schema.Schema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.util.Random

object HdfsPermissionsTestApp extends App with Logging {

  val conf = new HiveConf()
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  conf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(conf)
  implicit val ops = new HiveOps(client)
  implicit val fs = FileSystem.get(conf)

  val Database = "sam"
  val Table = "permissions"

  val data = Array(
    Vector("elton", "yellow brick road ", "1972"),
    Vector("elton", "tumbleweed connection", "1974"),
    Vector("elton", "empty sky", "1969"),
    Vector("beatles", "white album", "1969"),
    Vector("beatles", "tumbleweed connection", "1966"),
    Vector("pinkfloyd", "the wall", "1979"),
    Vector("pinkfloyd", "dark side of the moon", "1974"),
    Vector("pinkfloyd", "emily", "1966")
  )

  val rows = List.fill(100)(data(Random.nextInt(data.length)))
  val frame = Frame.fromValues(Schema("artist", "album", "year"), rows)

  new HiveOps(client).createTable(
    Database,
    Table,
    frame.schema,
    List("artist"),
    format = HiveFormat.Parquet,
    overwrite = true
  )

  val sink = HiveSink(Database, Table).withIOThreads(4)
  frame.to(sink)
  logger.info("Write complete")

  val permissions = HdfsSource("hdfs://localhost:9000/user/hive/warehouse/sam.db/*").permissions()
  println(permissions)
}
