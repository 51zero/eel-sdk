package io.eels.component.jdbc

import java.sql.DriverManager

import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class BucketPartitionStrategyTest extends WordSpec with Matchers {

  private val conn = DriverManager.getConnection("jdbc:h2:mem:test")
  conn.createStatement().executeUpdate("create table bucket_test (a integer)")
  for (k <- 0 until 20) {
    conn.createStatement().executeUpdate(s"insert into bucket_test (a) values (${Random.nextInt(10000)})")
  }

  "BucketPartitionStrategy" should {
    "generate evenly spaced ranges" in {
      BucketPartitionStrategy("a", 10, 2, 29).ranges shouldBe List((2, 4), (5, 7), (8, 10), (11, 13), (14, 16), (17, 19), (20, 22), (23, 25), (26, 27), (28, 29))
      BucketPartitionStrategy("a", 2, 2, 30).ranges shouldBe List((2, 16), (17, 30))
      BucketPartitionStrategy("a", 1, 4, 5).ranges shouldBe List((4, 5))
      BucketPartitionStrategy("a", 1, 4, 4).ranges shouldBe List((4, 4))
      BucketPartitionStrategy("a", 6, 1, 29).ranges shouldBe List((1, 5), (6, 10), (11, 15), (16, 20), (21, 25), (26, 29))
    }
    "return correct number of ranges" in {
      JdbcSource(() => conn, "select * from bucket_test")
        .withPartitionStrategy(BucketPartitionStrategy("a", 4, 0, 10000))
        .parts().size shouldBe 4
    }
    "return full and non overlapping data" in {
      JdbcSource(() => conn, "select * from bucket_test")
        .withPartitionStrategy(BucketPartitionStrategy("a", 4, 0, 10000))
        .toDataStream().collect.size shouldBe 20
    }
  }
}
