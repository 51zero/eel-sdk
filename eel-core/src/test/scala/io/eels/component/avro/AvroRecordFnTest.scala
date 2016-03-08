package io.eels.component.avro

import com.typesafe.config.ConfigFactory
import io.eels.{Column, Schema, SchemaType}
import org.scalatest.{Matchers, WordSpec}

class AvroRecordFnTest extends WordSpec with Matchers {

  val config = ConfigFactory.parseString("""  eel.avro.fillMissingValues : true  """)

  "AvroRecordFn" should {
    "replace missing values if flag set" in {
      val schema = AvroSchemaFn.toAvro(Schema("a", "b", "c"))
      AvroRecordFn.toRecord(Seq("1", "3"), schema, Schema("a", "c"), config).toString shouldBe
        """{"a": "1", "b": null, "c": "3"}"""
    }
    "convert values to booleans" in {
      val sourceSchema = Schema(Column("a", SchemaType.Boolean, true))
      val avroSchema = AvroSchemaFn.toAvro(sourceSchema)
      AvroRecordFn.toRecord(Seq("true"), avroSchema, sourceSchema, config).toString shouldBe """{"a": true}"""
    }
    "convert values to doubles" in {
      val sourceSchema = Schema(Column("a", SchemaType.Double, true))
      val avroSchema = AvroSchemaFn.toAvro(sourceSchema)
      AvroRecordFn.toRecord(Seq("13.3"), avroSchema, sourceSchema, config).toString shouldBe """{"a": 13.3}"""
    }
  }
}
