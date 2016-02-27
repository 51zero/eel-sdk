package io.eels.component.hive

import java.util.UUID

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.eels.{Schema, Frame}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.util.Random

object HiveBenchmarkApp extends App with StrictLogging {

  import scala.concurrent.ExecutionContext.Implicits.global

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

  val schema = Schema("id", "state")
  val rows = List.fill(10000)(List(UUID.randomUUID.toString, states(Random.nextInt(50))))

  logger.info(s"Generated ${rows.size} rows")

  HiveOps.createTable(
    "sam",
    "people",
    schema,
    List("state"),
    format = HiveFormat.Parquet,
    overwrite = true
  )

  logger.info("Table created")

  val sink = HiveSink("sam", "people").withDynamicPartitioning(true).withIOThreads(4)
  Frame(schema, rows).to(sink)

  logger.info("Write complete")

  val start = System.currentTimeMillis()

  val result = HiveSource("sam", "people")
    .withPartitionConstraint("state", "<=", "Iowa")
    .withColumns("id", "foo", "woo").toFrame(4).toSeq

  val end = System.currentTimeMillis()

  import scala.concurrent.duration._

  val duration = (end - start).millis

  logger.info(s"Read complete ${result.size} results")
  logger.info(s"Read took === ${duration.toSeconds} $duration")
  result.take(100).foreach { row =>
    logger.info("row=" + row)
  }
}
