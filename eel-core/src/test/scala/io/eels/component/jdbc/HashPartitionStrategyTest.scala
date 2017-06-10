package io.eels.component.jdbc

import java.sql.DriverManager

import org.scalatest.{Matchers, WordSpec}

class HashPartitionStrategyTest extends WordSpec with Matchers {

  private val conn = DriverManager.getConnection("jdbc:h2:mem:bucket_test")
  conn.createStatement().executeUpdate("create table mytable (a integer)")
  for (k <- 0 until 20) {
    conn.createStatement().executeUpdate(s"insert into mytable (a) values ($k)")
  }

  "HashPartitionStrategy" should {
    "return correct number of ranges" in {
      JdbcSource(() => conn, "select * from mytable")
        .withPartitionStrategy(HashPartitionStrategy("mod(a)", 10))
        .parts().size shouldBe 10
    }
    "return full and non overlapping data" in {
      JdbcSource(() => conn, "select * from mytable")
        .withPartitionStrategy(HashPartitionStrategy("mod(a, 10)", 10))
        .toFrame().collect().flatMap(_.values).toSet shouldBe
        Vector.tabulate(20) { k => k }.toSet
    }
  }
}
