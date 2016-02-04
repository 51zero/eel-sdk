package io.eels.component.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.scalatest.WordSpec

class HiveSourceTest extends WordSpec {

  val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hive = new HiveConf()
  hive.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  hive.reloadConfiguration()

  "HiveSource" should {
    "locate table in hive" ignore {
      println(HiveSource("sam", "news").toList)
    }
  }
}
