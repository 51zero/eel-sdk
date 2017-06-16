package io.eels.component.hive

import java.util.UUID

import com.sksamuel.exts.metrics.Timed
import io.eels.Frame
import io.eels.schema.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.util.Random

object HiveBenchmarkApp extends App with Timed {

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
    "Wyoming").map(_.replace(' ', '_').toLowerCase)

  val conf = new Configuration
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/home/sam/development/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"))
  conf.reloadConfiguration()

  implicit val fs = FileSystem.get(conf)

  implicit val hiveConf = new HiveConf()
  hiveConf.addResource(new Path("/home/sam/development/hive-1.2.1-bin/conf/hive-site.xml"))
  hiveConf.reloadConfiguration()

  implicit val client = new HiveMetaStoreClient(hiveConf)

  val schema = StructType("id", "state")
  val rows = List.fill(100000)(List(UUID.randomUUID.toString, states(Random.nextInt(50))))

  logger.info(s"Generated ${rows.size} rows")

  new HiveOps(client).createTable(
    "sam",
    "people",
    schema,
    List("state"),
    format = HiveFormat.Parquet,
    overwrite = true
  )

  logger.info("Table created")

  val sink = HiveSink("sam", "people").withDynamicPartitioning(true).withIOThreads(4)
  Frame.fromValues(schema, rows).to(sink)

  logger.info("Write complete")

  while (true) {

    timed("frame took") {
      val result = HiveSource("sam", "people").toFrame().collect
      println(result.size)
    }

    timed("datastream took") {
      val result = HiveSource("sam", "people").toDataStream().collect
      println(result.size)
    }
  }
}
