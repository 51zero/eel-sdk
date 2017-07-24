package io.eels.component.hive

import java.io.File

import io.eels.datastream.DataStream
import io.eels.schema.{Field, PartitionConstraint, StringType, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.Try

class HivePartitionConstraintTest extends FunSuite with Matchers with BeforeAndAfterAll {

  import HiveConfig._

  val dbname = "sam"
  val table = "constraints_test_" + System.currentTimeMillis()

  override def afterAll(): Unit = Try {
    HiveTable(dbname, table).drop()
  }

  val schema = StructType(
    Field("state", StringType),
    Field("city", StringType)
  )

  Try {
    DataStream.fromValues(schema, Seq(
      Seq("iowa", "des moines"),
      Seq("iowa", "iow city"),
      Seq("maine", "augusta")
    )).to(HiveSink(dbname, table).withCreateTable(true, Seq("state")))
  }

  test("hive source with partition constraint should return matching data") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

    HiveSource(dbname, table)
      .addPartitionConstraint(PartitionConstraint.equals("state", "iowa"))
      .toDataStream()
      .collect.size shouldBe 2
  }

  test("hive source with non-existing partitions in constraint should return no data") {
    assume(new File("/home/sam/development/hadoop-2.7.2/etc/hadoop/core-site.xml").exists)

    HiveSource(dbname, table)
      .addPartitionConstraint(PartitionConstraint.equals("state", "pa"))
      .toDataStream()
      .collect.size shouldBe 0
  }
}
