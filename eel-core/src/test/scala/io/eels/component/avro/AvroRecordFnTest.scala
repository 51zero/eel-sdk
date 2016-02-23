package io.eels.component.avro

import com.typesafe.config.ConfigFactory
import io.eels.FrameSchema
import org.scalatest.{Matchers, WordSpec}

class AvroRecordFnTest extends WordSpec with Matchers {

  "AvroRecordFn" should {
    "replace missing values flag set" in {
      val config = ConfigFactory.parseString("""  eel.avro.fillMissingValues : true  """)
      val schema = AvroSchemaGen(FrameSchema("a", "b", "c"))
      AvroRecordFn.toRecord(Seq("1", "3"), schema, FrameSchema("a", "c"), config).toString shouldBe
        """{"a": "1", "b": null, "c": "3"}"""
    }
  }
}
