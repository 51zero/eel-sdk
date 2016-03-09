package io.eels.testkit

import java.nio.file.Files
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.{FieldSchema, _}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class InMemoryMetaStoreClientTest extends WordSpec with Matchers {

  val dir = Files.createTempDirectory("databases-tests")
  val fs = FileSystem.getLocal(new Configuration)

  "InMemoryMetaStoreClientTest.createDatabase" should {
    "create database home folder" in {
      val client = new InMemoryMetaStoreClient(dir.toString, fs)
      val db = new Database("test", "", "", Map.empty[String, String].asJava)
      client.createDatabase(db)
      client.getDatabase("test") shouldBe db
      dir.resolve("test").toFile.exists shouldBe true
    }
  }

  "InMemoryMetaStoreClientTest.getDatabase" should {
    "return database if found" in {
      val client = new InMemoryMetaStoreClient(dir.toString, fs)
      val db = new Database("test", "", "", Map.empty[String, String].asJava)
      client.createDatabase(db)
      client.getDatabase("test") shouldBe db
      intercept[NoSuchObjectException] {
        client.getDatabase("qweqwe") shouldBe null
      }
    }
  }

  "InMemoryMetaStoreClientTest.getDatabases(databasePattern: String)" should {
    "return databases matching pattern" in {
      val client = new InMemoryMetaStoreClient(dir.toString, fs)
      val db1 = new Database("qwerty", "", "", Map.empty[String, String].asJava)
      val db2 = new Database("qwert", "", "", Map.empty[String, String].asJava)
      val db3 = new Database("asdfg", "", "", Map.empty[String, String].asJava)
      client.createDatabase(db1)
      client.createDatabase(db2)
      client.createDatabase(db3)
      client.getDatabases("qwe*").asScala.toSet shouldBe Set("qwerty", "qwert")
    }
  }

  "InMemoryMetaStoreClientTest.getAllDatabases" should {
    "return all existing database names" in {
      val client = new InMemoryMetaStoreClient(dir.toString, fs)
      val db1 = new Database("test1", "", "", Map.empty[String, String].asJava)
      val db2 = new Database("test2", "", "", Map.empty[String, String].asJava)
      client.createDatabase(db1)
      client.createDatabase(db2)
      client.getAllDatabases.asScala.toSet shouldBe Set("test1", "test2")
    }
  }

  "InMemoryMetaStoreClientTest.dropDatabase" should {
    "remove database" in {
      val client = new InMemoryMetaStoreClient(dir.toString, fs)
      val db1 = new Database("test1", "", "", Map.empty[String, String].asJava)
      val db2 = new Database("test2", "", "", Map.empty[String, String].asJava)
      client.createDatabase(db1)
      client.createDatabase(db2)
      client.dropDatabase("test1")
      client.getAllDatabases.asScala.toSet shouldBe Set("test2")
      client.dropDatabase("test2")
      client.getAllDatabases.asScala shouldBe Nil
    }
  }

  "InMemoryMetaStoreClientTest.createTable" should {
    "create table folder using sd location inside database" in {

      val tablePath = dir.resolve("sam").resolve("tabloc")

      val sd = new StorageDescriptor()
      sd.setCols(util.Arrays.asList(new FieldSchema("foo", "string", null)))
      sd.setSerdeInfo(new SerDeInfo(null, "serdeclass", new util.HashMap))
      sd.setInputFormat("inputformat")
      sd.setOutputFormat("outputformat")
      sd.setLocation(tablePath.toString)

      val table = new Table()
      table.setDbName("sam")
      table.setTableName("tab")
      table.setSd(sd)
      table.setTableType(TableType.MANAGED_TABLE.name)

      val client = new InMemoryMetaStoreClient(dir.toString, fs)
      client.createDatabase(new Database("sam", "", "", Map.empty[String, String].asJava))
      client.createTable(table)

      tablePath.toFile.exists shouldBe true
    }
  }
}
