package io.eels.component.hive

import org.scalatest.{Matchers, WordSpec}

class HiveFormatTest extends WordSpec with Matchers {

  "HiveFormat" should {
    "detect format from string" in {
      HiveFormat("text") shouldBe HiveFormat.Text
      HiveFormat("parquet") shouldBe HiveFormat.Parquet
      HiveFormat("orc") shouldBe HiveFormat.Orc
      HiveFormat("avro") shouldBe HiveFormat.Avro
    }
  }
}
