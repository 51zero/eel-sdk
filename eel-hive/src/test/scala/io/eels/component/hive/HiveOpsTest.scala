//package io.eels.component.hive
//
//import java.util
//
//import io.eels.schema.{Field, FieldType, Schema}
//import io.eels.testkit.HiveTestKit
//import org.apache.hadoop.hive.metastore.api.{Database, FieldSchema, StorageDescriptor, Table}
//import org.scalatest.{Matchers, WordSpec}
//
//import scala.collection.JavaConverters._
//
//class HiveOpsTest extends WordSpec with Matchers with HiveTestKit {
//
//  "HiveOps.tableFormat" should {
//    "return table format from hive meta store" in {
//
//      val table = new Table()
//      table.setDbName("db")
//      table.setTableName("tywin")
//      table.setSd(new StorageDescriptor())
//      table.getSd.setInputFormat("testformat")
//
//      client.createDatabase(new Database("db", "", "", new util.HashMap))
//      client.createTable(table)
//
//      new HiveOps(client).tableFormat("db", "tywin") shouldBe "testformat"
//    }
//  }
//
//  "HiveOps.location" should {
//    "return table location from hive meta store" in {
//
//      val table = new Table()
//      table.setDbName("db")
//      table.setTableName("brianne")
//      table.setSd(new StorageDescriptor())
//      table.getSd.setLocation("mytestlocation")
//      table.getSd.setInputFormat("testformat")
//
//      client.createDatabase(new Database("db", "", "", new util.HashMap))
//      client.createTable(table)
//
//      new HiveOps(client).location("db", "brianne") shouldBe "mytestlocation"
//    }
//  }
//
//  "HiveOps.schema" should {
//    "set fields to non null and set partitions to non null" in {
//
//      val table = new Table()
//      table.setDbName("db")
//      table.setTableName("jaime")
//      table.setSd(new StorageDescriptor())
//      table.getSd.setInputFormat("testformat")
//      table.getSd.setCols(List(new FieldSchema("p", "string", null), new FieldSchema("q", "string", null)).asJava)
//      table.setPartitionKeys(List(new FieldSchema("a", "string", null), new FieldSchema("b", "string", null)).asJava)
//
//      client.createDatabase(new Database("db", "", "", new util.HashMap))
//      client.createTable(table)
//
//      new HiveOps(client).schema("db", "jaime") shouldBe
//        Schema(
//          Field("p", FieldType.String, nullable = true),
//          Field("q", FieldType.String, nullable = true),
//          Field("a", FieldType.String, nullable = false, partition = true),
//          Field("b", FieldType.String, nullable = false, partition = true)
//        )
//    }
//  }
//}
