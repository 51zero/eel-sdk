package io.eels.component.jdbc

import java.sql.DriverManager

import org.scalatest.{Matchers, WordSpec}

class HashPartitionStrategyTest extends WordSpec with Matchers {

  private val conn = DriverManager.getConnection("jdbc:h2:mem:bucket_test")
  conn.createStatement().executeUpdate("create table hash_test (a integer)")
  for (k <- 0 until 20) {
    conn.createStatement().executeUpdate(s"insert into hash_test (a) values ($k)")
  }

  "HashPartitionStrategy" should {
    "return correct number of ranges" in {
      JdbcSource(() => DriverManager.getConnection("jdbc:h2:mem:bucket_test"), "select * from hash_test")
        .withPartitionStrategy(HashPartitionStrategy("mod(a)", 10))
        .parts().size shouldBe 10
    }
    "return full and non overlapping data" in {
      JdbcSource(() => DriverManager.getConnection("jdbc:h2:mem:bucket_test"), "select * from hash_test")
        .withPartitionStrategy(HashPartitionStrategy("mod(a, 10)", 10))
        .toDataStream().collect.flatMap(_.values).toSet shouldBe
        Vector.tabulate(20) { k => k }.toSet
    }
  }
}
