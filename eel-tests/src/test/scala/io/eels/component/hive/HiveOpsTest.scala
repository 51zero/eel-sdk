package io.eels.component.hive

import java.util

import io.eels.testkit.HiveTestKit
import org.apache.hadoop.hive.metastore.api.{Database, StorageDescriptor, Table}
import org.scalatest.{Matchers, WordSpec}

class HiveOpsTest extends WordSpec with Matchers with HiveTestKit {

  "HiveOps.tableFormat" should {
    "return table format from hive meta store" in {

      val table = new Table()
      table.setDbName("db")
      table.setTableName("tywin")
      table.setSd(new StorageDescriptor())
      table.getSd.setInputFormat("testformat")

      client.createDatabase(new Database("db", "", "", new util.HashMap))
      client.createTable(table)

      HiveOps.tableFormat("db", "tywin") shouldBe "testformat"
    }
  }

  "HiveOps.location" should {
    "return table location from hive meta store" in {

      val table = new Table()
      table.setDbName("db")
      table.setTableName("brianne")
      table.setSd(new StorageDescriptor())
      table.getSd.setLocation("mytestlocation")
      table.getSd.setInputFormat("testformat")

      client.createDatabase(new Database("db", "", "", new util.HashMap))
      client.createTable(table)

      HiveOps.location("db", "brianne") shouldBe "mytestlocation"
    }
  }
}
