package io.eels.component.orc

import io.eels.schema._
import org.apache.orc.TypeDescription
import org.scalatest.{FunSuite, Matchers}

// tests that the eel <-> orc schemas are compatible
class OrcSchemaCompatibilityTest extends FunSuite with Matchers {

  test("orc schemas should be cross compatible with eel structs") {

    val schema = TypeDescription.createStruct()
      .addField("binary", TypeDescription.createBinary())
      .addField("boolean", TypeDescription.createBoolean())
      .addField("byte", TypeDescription.createByte())
      .addField("char", TypeDescription.createChar().withMaxLength(8))
      .addField("date", TypeDescription.createDate())
      .addField("decimal", TypeDescription.createDecimal().withScale(2).withPrecision(4))
      .addField("double", TypeDescription.createDouble())
      .addField("float", TypeDescription.createFloat())
      .addField("int", TypeDescription.createInt())
      .addField("long", TypeDescription.createLong())
      .addField("timestamp", TypeDescription.createTimestamp())
      .addField("varchar", TypeDescription.createVarchar().withMaxLength(222))

    val structType = StructType(Vector(
      Field("binary", BinaryType, true),
      Field("boolean", BooleanType, true),
      Field("byte", ByteType.Signed, true),
      Field("char", CharType(8), true),
      Field("date", DateType, true),
      Field("decimal", DecimalType(4, 2), true),
      Field("double", DoubleType, true),
      Field("float", FloatType, true),
      Field("int", IntType.Signed, true),
      Field("long", LongType.Signed, true),
      Field("timestamp", TimestampMillisType, true),
      Field("varchar", VarcharType(222), true)
    ))

    OrcSchemaFns.fromOrcSchema(schema) shouldBe structType
    OrcSchemaFns.toOrcSchema(structType) shouldBe schema
  }
}
