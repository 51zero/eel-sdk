package io.eels.component.jdbc

import java.sql.DriverManager

import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class BucketPartitionTest extends WordSpec with Matchers {

  private val conn = DriverManager.getConnection("jdbc:h2:mem:bucket_test")
  conn.createStatement().executeUpdate("create table mytable (a integer)")
  for (k <- 0 until 20) {
    conn.createStatement().executeUpdate(s"insert into mytable (a) values (${Random.nextInt(10000)})")
  }

  "BucketPartitionStrategy" should {
    "generate evenly spaced ranges" in {
      BucketPartitionStrategy("a", 10, 2, 29).ranges shouldBe List(Range.inclusive(2, 4), Range.inclusive(5, 7), Range.inclusive(8, 10), Range.inclusive(11, 13), Range.inclusive(14, 16), Range.inclusive(17, 19), Range.inclusive(20, 22), Range.inclusive(23, 25), Range.inclusive(26, 27), Range.inclusive(28, 29))
      BucketPartitionStrategy("a", 2, 2, 30).ranges shouldBe List(Range.inclusive(2, 16), Range.inclusive(17, 30))
      BucketPartitionStrategy("a", 1, 4, 5).ranges shouldBe List(Range.inclusive(4, 5))
      BucketPartitionStrategy("a", 1, 4, 4).ranges shouldBe List(Range.inclusive(4, 4))
      BucketPartitionStrategy("a", 6, 1, 29).ranges shouldBe List(Range.inclusive(1, 5), Range.inclusive(6, 10), Range.inclusive(11, 15), Range.inclusive(16, 20), Range.inclusive(21, 25), Range.inclusive(26, 29))
    }
    "return correct number of ranges" in {
      JdbcSource(() => conn, "select * from mytable")
        .withPartitionStrategy(BucketPartitionStrategy("a", 4, 0, 10000))
        .parts().size shouldBe 4
    }
    "return full and non overlapping data" in {
      JdbcSource(() => conn, "select * from mytable")
        .withPartitionStrategy(BucketPartitionStrategy("a", 4, 0, 10000))
        .toFrame().collect().size shouldBe 20
    }
  }
}
