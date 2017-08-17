package io.eels.component.jdbc

import java.sql.DriverManager

import org.scalatest.{Matchers, WordSpec}

class HashPartitionStrategyTest extends WordSpec with Matchers {

  Class.forName("org.h2.Driver")

  private val db = "hash_test"
  private val uri = s"jdbc:h2:mem:$db"
  private val conn = DriverManager.getConnection(uri)
  conn.createStatement().executeUpdate("create table hash_test (a integer)")
  for (k <- 0 until 20) {
    conn.createStatement().executeUpdate(s"insert into hash_test (a) values ($k)")
  }

  "HashPartitionStrategy" should {
    "return correct number of ranges" in {
      JdbcSource(() => DriverManager.getConnection(uri), "select * from hash_test")
        .withPartitionStrategy(HashPartitionStrategy("mod(a)", 10))
        .parts().size shouldBe 10
    }
    "return full and non overlapping data" in {
      JdbcSource(() => DriverManager.getConnection(uri), "select * from hash_test")
        .withPartitionStrategy(HashPartitionStrategy("mod(a, 10)", 10))
        .toDataStream().collect.flatMap(_.values).toSet shouldBe
        Vector.tabulate(20) { k => k }.toSet
    }
  }
}
