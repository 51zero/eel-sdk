//package io.eels.component.hive
//
//import io.eels.{Frame, Row}
//import io.eels.schema.Schema
//import io.eels.testkit.HiveTestKit
//import org.scalatest.{Matchers, WordSpec}
//
//class HiveFilesFnTest extends WordSpec with Matchers with HiveTestKit {
//
//  val schema = Schema("a", "b")
//  val data = Seq.fill(1000)(Seq(Row(schema, "1", "2"), Row(schema, "2", "3"), Row(schema, "3", "4"))).flatten
//
//  "HiveFilesFn" should {
//    "scan table root on non partitioned table" ignore {
//
//      new HiveOps(client).createTable("sam", "hivefilesfn1", schema, Nil, format = HiveFormat.Parquet)
//      Frame(schema, data).to(HiveSink("sam", "hivefilesfn1").withIOThreads(2))
//
//      val table = client.getTable("sam", "hivefilesfn1")
//
//      // we should have 2 files, one per thread, as data is written straight to the table root
//      HiveFilesFn(table, Nil).size shouldBe 2
//    }
//    "scan partition paths for partitioned table" ignore {
//
//      new HiveOps(client).createTable("sam", "hivefilesfn2", schema, partitionKeys = List("a"), HiveFormat.Parquet)
//      Frame(schema, data).to(HiveSink("sam", "hivefilesfn2").withIOThreads(2))
//
//      val table = client.getTable("sam", "hivefilesfn2")
//
//      // we should have 6 files, 3 partition values, and then 2 files for each partition (2 threads)
//      HiveFilesFn(table, Nil).size shouldBe 6
//    }
//  }
//}
