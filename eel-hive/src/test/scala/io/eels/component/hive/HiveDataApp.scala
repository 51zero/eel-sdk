package io.eels.component.hive

import java.util.UUID

import com.sksamuel.exts.metrics.Timed
import io.eels.{Listener, Row}
import io.eels.datastream.DataStream
import io.eels.schema.{BooleanType, Field, IntType, StringType, StructType}
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

  val ops = HiveTable(Database, Table)
  ops.drop()

  val schema = StructType(
    Field("id", StringType),
    Field("name", StringType),
    Field("state", StringType),
    Field("population", IntType.Signed),
    Field("incorporated", BooleanType)
  )
  def createRow = Row(schema, Seq(UUID.randomUUID.toString, List.fill(8)(Random.nextPrintableChar).mkString, states(Random.nextInt(50)), Random.nextInt(1000000), Random.nextBoolean))

  val sink = HiveSink(Database, Table)
    .withCreateTable(true)

  val size = 100000

  DataStream.fromIterator(
    schema,
    Iterator.continually(createRow).take(size)
  ).listener(new Listener {
    var count = 0
    override def onNext(row: Row): Unit = {
      count = count + 1
      if (count % 10000 == 0) logger.info("Count=" + count)
    }
  })
    .to(sink)

  logger.info("Write complete")

  val table = new HiveOps(client).tablePath(Database, Table)
  logger.info("table:" + table)

  val partitions = new HiveOps(client).hivePartitions(Database, Table)
  logger.info("Partitions:" + partitions)

  val rows = HiveSource(Database, Table).toDataStream().collect
  println(rows.take(20))

  assert(rows.size == size)

  logger.info("Row count from stats: " + HiveTable(Database, Table).stats())

}
