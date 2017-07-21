package io.eels.component.hive

import java.io.File
import java.util.UUID

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{BooleanType, Field, IntType, StringType, StructType}
import org.scalatest.{FunSuite, Matchers}

import scala.util.Random

class HiveStatsTest extends FunSuite with Matchers with HiveConfig {

  val dbname = "sam"
  val table = "stats_test_" + System.currentTimeMillis()

  test("hive table should return stats via parquet footers") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)
    HiveTable(dbname, table).drop()

    val schema = StructType(
      Field("a", StringType)
    )
    def createRow = Row(schema, Seq(UUID.randomUUID.toString))

    val sink = HiveSink(dbname, table).withCreateTable(true)
    val size = 100000

    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).to(sink)
    HiveTable(dbname, table).stats().count shouldBe size
  }

  test("hive table should return partition counts") {
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

    //    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).to(sink)
    //    HiveTable(dbname, table).stats().count.toSeq shouldBe
    //      Seq(
    //        Partition(Seq(PartitionEntry("a", "a"))),
    //        Partition(Seq(PartitionEntry("a", "b"))),
    //        Partition(Seq(PartitionEntry("a", "c"))),
    //        Partition(Seq(PartitionEntry("a", "d")))
    //      )
    //    HiveTable(dbname, table).stats().partitions.values.map(_.rows).sum shouldBe size
  }

   test("return min and max") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

    val schema = StructType(
      Field("a", StringType),
      Field("b", IntType.Signed)
    )
    def createRow = Row(schema, Seq(Random.shuffle(List("a", "b", "c")).head, Random.shuffle(List(1, 2, 3, 4, 5)).head))

    val sink = HiveSink(dbname, table).withCreateTable(true)
    val size = 10000

    DataStream.fromIterator(schema, Iterator.continually(createRow).take(size)).to(sink, 10)

    HiveTable(dbname, table).stats.max("b") shouldBe 5
    HiveTable(dbname, table).stats.min("b") shouldBe 1
  }
}
