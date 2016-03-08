package io.eels.testkit

import org.apache.hadoop.hive.metastore.api.{Database, NoSuchObjectException}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class InMemoryMetaStoreClientTest extends WordSpec with Matchers {

  "InMemoryMetaStoreClientTest.getDatabase" should {
    "return database if found" in {
      val client = new InMemoryMetaStoreClient()
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
      val client = new InMemoryMetaStoreClient()
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
      val client = new InMemoryMetaStoreClient()
      val db1 = new Database("test1", "", "", Map.empty[String, String].asJava)
      val db2 = new Database("test2", "", "", Map.empty[String, String].asJava)
      client.createDatabase(db1)
      client.createDatabase(db2)
      client.getAllDatabases.asScala.toSet shouldBe Set("test1", "test2")
    }
  }

  "InMemoryMetaStoreClientTest.dropDatabase" should {
    "remove database" in {
      val client = new InMemoryMetaStoreClient()
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
}
