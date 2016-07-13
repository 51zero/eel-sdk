package io.eels.component.avro

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.eels.schema.Field
import io.eels.Row
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import org.apache.avro.generic.GenericData

class AvroRecordDeserializerTest : WordSpec() {

  val config: Config = ConfigFactory.parseString("""  eel.avro.fillMissingValues : true  """)

  init {
    "toRow" should {
      "create eel row from supplied avro record" {
        val schema = Schema(Field("a"), Field("b"), Field("c"))
        val record = GenericData.Record(toAvroSchema(schema))
        record.put("a", "aaaa")
        record.put("b", "bbbb")
        record.put("c", "cccc")
        AvroRecordDeserializer().toRow(record) shouldBe Row(schema, listOf("aaaa", "bbbb", "cccc"))
      }
      "return row in same order as target schema" {
        val schema = Schema(Field("a"), Field("b"), Field("c"))
        val targetSchema = Schema(Field("c"), Field("b"), Field("a"))
        val record = GenericData.Record(toAvroSchema(schema))
        record.put("a", "aaaa")
        record.put("b", "bbbb")
        record.put("c", "cccc")
        AvroRecordDeserializer().toRow(record) shouldBe Row(targetSchema, listOf("cccc", "bbbb", "aaaa"))
      }
    }
  }
}
