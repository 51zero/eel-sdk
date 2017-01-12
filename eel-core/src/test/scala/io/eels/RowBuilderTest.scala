package io.eels

import io.eels.schema.{Field, StringType, StructType}
import org.scalatest.{FlatSpec, Matchers}

class RowBuilderTest extends FlatSpec with Matchers {

  "RowBuilder" should "create row with appropriate fields" in {
    val structType = StructType(
      Field("name", StringType, nullable = false),
      Field("location", StringType, nullable = false)
    )
    val builder = new RowBuilder(structType)
    builder.put(0, "Romulan")
    builder.put(1, "Romulus")
    builder.build() shouldBe Row(structType, Vector("Romulan", "Romulus"))
  }

  it should "support skipping indexes replacing those with null" in {
    val structType = StructType(
      Field("name", StringType, nullable = false),
      Field("location", StringType, nullable = false)
    )
    val builder = new RowBuilder(structType)
    builder.put(0, "Romulan")
    builder.build() shouldBe Row(structType, Vector("Romulan", null))
  }
}
