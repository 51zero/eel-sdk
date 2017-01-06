package io.eels.component.avro

import io.eels.schema._
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.scalatest.{FunSuite, Matchers}

// tests that the eel <-> avro schemas are compatible
class AvroSchemaCompatibilityTest extends FunSuite with Matchers {

  test("avro record schemas should be cross compatible with eel struct types") {

    val decimal = Schema.create(Schema.Type.BYTES)
    LogicalTypes.decimal(14, 7).addToSchema(decimal)

    val timestampMillis = Schema.create(Schema.Type.LONG)
    LogicalTypes.timestampMillis().addToSchema(timestampMillis)

    val schema = SchemaBuilder.record("row").namespace("namespace").fields()
      .optionalBoolean("optbool")
      .optionalBytes("optbytes")
      .optionalDouble("optdouble")
      .optionalFloat("optfloat")
      .optionalInt("optint")
      .optionalLong("optlong")
      .optionalString("optstring")
      .requiredBoolean("reqbool")
      .requiredBytes("reqbytes")
      .requiredDouble("reqdouble")
      .requiredFloat("reqfloat")
      .requiredInt("reqint")
      .requiredLong("reqlong")
      .requiredString("reqstring")
      .name("reqdecimal").`type`(decimal).noDefault()
      .name("reqtimestamp").`type`(timestampMillis).noDefault()
      .endRecord()

    val structType = StructType(Vector(
      Field("optbool", BooleanType, true, false, None, Map()),
      Field("optbytes", BinaryType, true, false, None, Map()),
      Field("optdouble", DoubleType, true, false, None, Map()),
      Field("optfloat", FloatType, true, false, None, Map()),
      Field("optint", IntType(true), true, false, None, Map()),
      Field("optlong", LongType(true), true, false, None, Map()),
      Field("optstring", StringType, true, false, None, Map()),
      Field("reqbool", BooleanType, false, false, None, Map()),
      Field("reqbytes", BinaryType, false, false, None, Map()),
      Field("reqdouble", DoubleType, false, false, None, Map()),
      Field("reqfloat", FloatType, false, false, None, Map()),
      Field("reqint", IntType(true), false, false, None, Map()),
      Field("reqlong", LongType(true), false, false, None, Map()),
      Field("reqstring", StringType, false, false, None, Map()),
      Field("reqdecimal", DecimalType(14, 7), false, false, None, Map()),
      Field("reqtimestamp", TimestampType, false, false, None, Map())
    ))

    AvroSchemaFns.fromAvroSchema(schema) shouldBe structType
    AvroSchemaFns.toAvroSchema(structType) shouldBe schema
  }
}
