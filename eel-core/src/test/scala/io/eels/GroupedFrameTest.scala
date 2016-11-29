package io.eels

import io.eels.schema.{Field, IntType, LongType, StructType}
import org.scalatest.{Matchers, WordSpec}

class GroupedFrameTest extends WordSpec with Matchers {

  val schema = StructType(
    Field("artist"),
    Field("year", IntType()),
    Field("album"),
    Field("sales", LongType())
  )
  val frame = Frame.apply(schema,
    Row(schema, Vector("Elton John", 1969, "Empty Sky", 1433)),
    Row(schema, Vector("Elton John", 1971, "Madman Across the Water", 7636)),
    Row(schema, Vector("Elton John", 1972, "Honky Château", 2525)),
    Row(schema, Vector("Elton John", 1973, "Goodbye Yellow Brick Road", 4352)),
    Row(schema, Vector("Elton John", 1975, "Rock of the Westies", 5645)),
    Row(schema, Vector("Kate Bush", 1978, "The Kick Inside", 2577)),
    Row(schema, Vector("Kate Bush", 1978, "Lionheart", 745)),
    Row(schema, Vector("Kate Bush", 1980, "Never for Ever", 7444)),
    Row(schema, Vector("Kate Bush", 1982, "The Dreaming", 8253)),
    Row(schema, Vector("Kate Bush", 1985, "Hounds of Love", 2495))
  )

  "grouped operations" should {
    "support sum" in {
      frame.groupBy("artist").sum("sales").toFrame().toSet().map(_.values) shouldBe
        Set(Vector("Elton John", 21591), Vector("Kate Bush", 21514.0))
    }
    "support count" in {
      frame.groupBy("artist").count("album").toFrame().toSet().map(_.values) shouldBe
        Set(Vector("Elton John", 5), Vector("Kate Bush", 5))
    }
    "support avg" in {
      frame.groupBy("artist").avg("sales").toFrame().toSet().map(_.values) shouldBe
        Set(Vector("Elton John", 4318.2), Vector("Kate Bush", 4302.8))
    }
    "support min" in {
      frame.groupBy("artist").min("year").toFrame().toSet().map(_.values) shouldBe
        Set(Vector("Elton John", 1969), Vector("Kate Bush", 1978))
    }
    "support max" in {
      frame.groupBy("artist").max("year").toFrame().toSet().map(_.values) shouldBe
        Set(Vector("Elton John", 1975), Vector("Kate Bush", 1985))
    }
    "support multiple aggregations" in {
      frame.groupBy("artist").avg("year").sum("sales").toFrame().toSet().map(_.values) shouldBe
        Set(Vector("Elton John", 1972.0, 21591.0), Vector("Kate Bush", 1980.6, 21514.0))
    }
  }
}
