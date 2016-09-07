package io.eels.component.avro

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.eels.schema.Field
import io.eels.Row
import io.eels.schema.Schema
import org.apache.avro.generic.GenericData
import org.scalatest.{Matchers, WordSpec}

class AvroRecordDeserializerTest extends WordSpec with Matchers {

  val config: Config = ConfigFactory.parseString("""  eel.avro.fillMissingValues : true  """)

  "toRow" should {
    "createReader eel row from supplied avro record" in {
      val schema = Schema(Field("a"), Field("b"), Field("c"))
      val record = new GenericData.Record(AvroSchemaFns.toAvroSchema(schema))
      record.put("a", "aaaa")
      record.put("b", "bbbb")
      record.put("c", "cccc")
      new AvroRecordDeserializer().toRow(record) shouldBe Row(schema, "aaaa", "bbbb", "cccc")
    }
  }
}
