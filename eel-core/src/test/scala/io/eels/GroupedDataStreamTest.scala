package io.eels

import io.eels.datastream.DataStream
import io.eels.schema.{Field, IntType, LongType, StructType}
import org.scalatest.{Matchers, WordSpec}

class GroupedDataStreamTest extends WordSpec with Matchers {

  val schema = StructType(
    Field("artist"),
    Field("year", IntType()),
    Field("album"),
    Field("sales", LongType())
  )
  val ds = DataStream.fromRows(schema,
    Array("Elton John", 1969, "Empty Sky", 1433),
    Array("Elton John", 1971, "Madman Across the Water", 7636),
    Array("Elton John", 1972, "Honky Château", 2525),
    Array("Elton John", 1973, "Goodbye Yellow Brick Road", 4352),
    Array("Elton John", 1975, "Rock of the Westies", 5645),
    Array("Kate Bush", 1978, "The Kick Inside", 2577),
    Array("Kate Bush", 1978, "Lionheart", 745),
    Array("Kate Bush", 1980, "Never for Ever", 7444),
    Array("Kate Bush", 1982, "The Dreaming", 8253),
    Array("Kate Bush", 1985, "Hounds of Love", 2495)
  )

  "grouped operations" should {
    "support sum" ignore {
      ds.groupBy("artist").sum("sales").toDataStream.toSet shouldBe
        Set(Vector("Elton John", 21591), Vector("Kate Bush", 21514.0))
    }
    "support count" ignore {
      ds.groupBy("artist").count("album").toDataStream.toSet shouldBe
        Set(Vector("Elton John", 5), Vector("Kate Bush", 5))
    }
    "support avg" ignore {
      ds.groupBy("artist").avg("sales").toDataStream.toSet shouldBe
        Set(Vector("Elton John", 4318.2), Vector("Kate Bush", 4302.8))
    }
    "support min" ignore {
      ds.groupBy("artist").min("year").toDataStream.toSet shouldBe
        Set(Vector("Elton John", 1969), Vector("Kate Bush", 1978))
    }
    "support max" ignore {
      ds.groupBy("artist").max("year").toDataStream.toSet shouldBe
        Set(Vector("Elton John", 1975), Vector("Kate Bush", 1985))
    }
    "support multiple aggregations" ignore {
      ds.groupBy("artist").avg("year").sum("sales").toDataStream.toSet shouldBe
        Set(Vector("Elton John", 1972.0, 21591.0), Vector("Kate Bush", 1980.6, 21514.0))
    }
    "support aggregations on entire dataset" ignore {
      ds.aggregated().avg("year").sum("sales").toDataStream.toSet shouldBe
        Set(Vector(1976.3, 43105.0))
    }
  }
}
