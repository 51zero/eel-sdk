package io.eels.component.hive

import java.util.UUID

import com.sksamuel.exts.metrics.Timed
import io.eels.datastream.DataStream
import io.eels.schema.{BooleanType, Field, IntType, StringType, StructType}
import io.eels.{Listener, Row}

import scala.util.Random

object HiveDataApp extends App with HiveConfig with Timed {

  private val Database = "sam"
  private val Table = "cities"
  private val Table2 = "cities2"

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

  val schema = StructType(
    Field("id", StringType),
    Field("name", StringType),
    Field("state", StringType),
    Field("population", IntType.Signed),
    Field("incorporated", BooleanType)
  )
  def createRow = Row(schema, Seq(UUID.randomUUID.toString, List.fill(8)(Random.nextPrintableChar).mkString, states(Random.nextInt(50)), Random.nextInt(1000000), Random.nextBoolean))

  val size = 1000 * 1000 * 100

  //  for (_ <- 1 to 5) {
  //    timed("Orc write complete") {
  //      HiveTable(Database, Table).drop()
  //
  //      val sink = HiveSink(Database, Table).withCreateTable(true, format = HiveFormat.Orc)
  //
  //      DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).listener(new Listener {
  //        var count = 0
  //        override def onNext(row: Row): Unit = {
  //          count = count + 1
  //          if (count % 10000 == 0) logger.info("Count=" + count)
  //        }
  //      }).to(sink, 4)
  //
  //      Thread.sleep(1000)
  //    }

//  timed("Parquet write complete") {
//    HiveTable(Database, Table).drop()
//
//
//    val sink = HiveSink(Database, Table).withCreateTable(true, format = HiveFormat.Parquet)
//
//    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).listener(new Listener {
//      var count = 0
//      override def onNext(row: Row): Unit = {
//        count = count + 1
//        if (count % 10000 == 0) logger.info("Count=" + count)
//      }
//    }).to(sink, 4)
//
//    Thread.sleep(1000)
//  }
  //  }

  val table = new HiveOps(client).tablePath(Database, Table)
  logger.info("table:" + table)

  val partitions = new HiveOps(client).hivePartitions(Database, Table)
  logger.info("Partitions:" + partitions)

  HiveTable(Database, Table2).drop()
  HiveSource(Database, Table).toDataStream.to(HiveSink(Database, Table2).withCreateTable(true))

  logger.info("Row count from stats: " + HiveTable(Database, Table).stats)
  logger.info("Row count from stats: " + HiveTable(Database, Table2).stats)

}
