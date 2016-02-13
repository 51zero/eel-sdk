package io.eels.component.hive

import org.scalatest.{WordSpec, Matchers}

class HiveDatasetUriTest extends WordSpec with Matchers {

  "HiveDatasetUri" should {
    "match string" in {
      HiveDatasetUri("hive:mydb:mytab") shouldBe HiveDatasetUri("mydb", "mytab")
    }
    "unapply string" in {
      "hive:mydb:mytab" match {
        case HiveDatasetUri(db, table) =>
          db shouldBe "mydb"
          table shouldBe "mytab"
        case _ => sys.error("failure")
      }
    }
  }
}
