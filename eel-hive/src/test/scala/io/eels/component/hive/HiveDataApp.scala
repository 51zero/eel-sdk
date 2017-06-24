package io.eels.component.hive

import java.util.UUID

import com.sksamuel.exts.metrics.Timed
import io.eels.datastream.DataStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

import scala.util.Random

object HiveDataApp extends App with Timed {

  private val Database = "sam"
  private val Table = "cities"

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

  case class City(id: String, name: String, state: String, population: Int, incorporated: Boolean)
  val cities = List.fill(30000) {
    City(UUID.randomUUID.toString, List.fill(8)(Random.nextPrintableChar).mkString, states(Random.nextInt(50)), Random.nextInt(1000000), Random.nextBoolean)
  }
  logger.info(s"Generated ${cities.size} rows")

  val sink = HiveSink(Database, Table)
    .withDynamicPartitioning(true)
    .withCreateTable(true, true, Seq("state", "incorporated"))
  DataStream(cities).to(sink)

  logger.info("Write complete")

  val table = new HiveOps(client).tablePath(Database, Table)
  println(table)

  println(HiveSource(Database, Table).partitions)

  val partitions = new HiveOps(client).hivePartitions(Database, Table)
  println(partitions)


}
