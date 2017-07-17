package io.eels.component.hive

import java.io.File
import java.util.UUID

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{BooleanType, Field, IntType, Partition, PartitionEntry, StringType, StructType}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class HiveStatsTest extends FunSuite with Matchers with HiveConfig {

  val dbname = "sam"
  val table = "stats_test_" + System.currentTimeMillis()

  test("hive table should return stats via parquet footers") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    HiveTable(dbname, table).drop()

    val schema = StructType(
      Field("a", StringType),
      Field("b", StringType),
      Field("c", IntType.Signed),
      Field("d", BooleanType)
    )
    def createRow = Row(schema, Seq(UUID.randomUUID.toString, UUID.randomUUID.toString, Random.nextInt(1000000), Random.nextBoolean))

    val sink = HiveSink(dbname, table).withCreateTable(true)
    val size = 100000

    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).to(sink)
    HiveTable(dbname, table).stats().rows shouldBe size
  }

  test("hive table should return partition stats via parquet footers") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    HiveTable(dbname, table).drop()

    val schema = StructType(
      Field("a", StringType),
      Field("b", StringType),
      Field("c", IntType.Signed),
      Field("d", BooleanType)
    )
    def createRow = Row(schema, Seq(Random.shuffle(Seq("a", "b", "c", "d")).head, UUID.randomUUID.toString, Random.nextInt(1000000), Random.nextBoolean))

    val sink = HiveSink(dbname, table).withCreateTable(true, Seq("a"))
    val size = 100000

    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).to(sink)
    HiveTable(dbname, table).stats().partitions.keys.toSeq shouldBe
      Seq(
        Partition(Seq(PartitionEntry("a", "a"))),
        Partition(Seq(PartitionEntry("a", "b"))),
        Partition(Seq(PartitionEntry("a", "c"))),
        Partition(Seq(PartitionEntry("a", "d")))
      )
    HiveTable(dbname, table).stats().partitions.values.map(_.rows).sum shouldBe size
  }
}
