package io.eels.component.hive

import java.io.File

import io.eels.datastream.DataStream
import io.eels.schema.{Field, Partition, PartitionEntry, StructType}
import org.scalatest.{FunSuite, Matchers}

class HiveDynamicPartitionTest extends FunSuite with HiveConfig with Matchers {

  val dbname = "sam"
  val table = "dynp_test"

  test("dynamic partition strategy should create new partitions") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

    val schema = StructType(Field("a"), Field("b"))

    // create the table with 'a' set as a partition, then try to persist a datastream with
    // different values for that partition
    HiveTable(dbname, table).drop()
    HiveTable(dbname, table).create(schema, Seq("a"))

    DataStream.fromValues(schema, Seq(Seq("1", "2"), Seq("3", "4"))).to(HiveSink(dbname, table))

    new HiveOps(client).partitions(dbname, table) shouldBe List(
      Partition(List(PartitionEntry("a", "1"))),
      Partition(List(PartitionEntry("a", "3")))
    )
  }
}
