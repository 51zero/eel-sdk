package io.eels.component.hive

import java.util.UUID

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Frame, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.util.Random

object HiveBenchmarkApp extends App with StrictLogging {

  val states = List(
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming")

  val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)

 // val rows = List.fill(1000000)(Row(Map("name" -> UUID.randomUUID.toString, "state" -> states(Random.nextInt(50)))))
 // val frame = Frame(rows)

//  HiveOps.createTable(
//    "sam",
//    "people",
//    frame.schema,
//    List("state"),
//    format = HiveFormat.Parquet,
//    overwrite = true
//  )
//
//  logger.info("Table created")
//
//  val sink = HiveSink("sam", "people").withPartitions("state").withIOThreads(1)
//  frame.to(sink).runConcurrent(2)
//
//  logger.info("Write complete")

  val start = System.currentTimeMillis()

  val result = HiveSource("sam", "people").withPartition("state", "<=", "Iowa").toFrame(4).toList.runConcurrent(4)

  val end = System.currentTimeMillis()

  import scala.concurrent.duration._

  val duration = (end - start).millis

  logger.info(s"Read complete ${result.size} results")
  logger.info(s"Read took === ${duration.toSeconds} $duration")
}
