package io.eels.component.avro

import com.typesafe.config.ConfigFactory
import io.eels.Column
import io.eels.Row
import io.eels.Schema
import io.kotlintest.specs.WordSpec
import org.apache.avro.generic.GenericData

class AvroRecordFnTest : WordSpec() {

  val config = ConfigFactory.parseString("""  eel.avro.fillMissingValues : true  """)

  init {
    "fromRecord" should {
      "create eel row from supplied avro record" with {
        val schema = Schema(Column("a"), Column("b"), Column("c"))
        val record = GenericData.Record(toAvro(schema))
        record.put("a", "aaaa")
        record.put("b", "bbbb")
        record.put("c", "cccc")
        fromRecord(record) shouldBe Row(schema, listOf("aaaa", "bbbb", "cccc"))
      }
    }
    //    "AvroRecordFn" should
    //        {
    //          "replace missing values if flag set" in {
    //            val schema = Schema(Column("a"), Column("b"), Column("c"))
    //            toRecord(listOf("1", "3"), schema, Schema(Column("a"), Column("c")), config).toString shouldBe
    //                """{"a": "1", "b": null, "c": "3"}"""
    //          }
    //          "convert values to booleans" in {
    //            val sourceSchema = Schema(Column("a", ColumnType.Boolean, true))
    //            val avroSchema = toAvro(sourceSchema)
    //            toRecord(Seq("true"), avroSchema, sourceSchema, config).toString shouldBe """{"a": true}"""
    //          }
    //          "convert values to doubles" in {
    //            val sourceSchema = Schema(Column("a", ColumnType.Double, true))
    //            val avroSchema = toAvro(sourceSchema)
    //            toRecord(Seq("13.3"), avroSchema, sourceSchema, config).toString shouldBe """{"a": 13.3}"""
    //          }
    //        }
    //
    //    "fromRecord" should        {
    //      "return record in same order as target schema" with {
    //        val schema = Schema(Column("a"), Column("b"), Column("c"))
    //        val targetSchema = Schema(Column("c"), Column("a"))
    //        val record = GenericData.Record(toAvro(schema))
    //        record.put("a", "aaaa")
    //        record.put("b", "bbbb")
    //        record.put("c", "cccc")
    //        fromRecord(record, toAvro(targetSchema)) shouldBe Vector("cccc", "aaaa")
    //      }
    //    }
  }
}
