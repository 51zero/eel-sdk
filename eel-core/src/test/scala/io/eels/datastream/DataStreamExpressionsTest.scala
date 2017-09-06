package io.eels.datastream

import io.eels.Row
import io.eels.schema.{DoubleType, Field, IntType, LongType, StructType}
import org.scalatest.{FlatSpec, Matchers}

class DataStreamExpressionsTest extends FlatSpec with Matchers {

  val schema = StructType(
    Field("artist"),
    Field("year", IntType()),
    Field("album"),
    Field("sales", LongType())
  )
  val ds = DataStream.fromRows(schema,
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

  "DataStream.filter" should "support expressions" in {
    import io.eels._
    ds.filter(select("album") === "Lionheart")
      .collectValues shouldBe Vector(Vector("Kate Bush", 1978, "Lionheart", 745))
  }

  "DataStream.addField" should "support multiply expressions" in {
    import io.eels._
    ds.filter(select("album") === "Lionheart")
      .addField(Field("woo", DoubleType), select("year") * 1.2)
      .collectValues shouldBe Vector(Vector("Kate Bush", 1978, "Lionheart", 745, BigDecimal(2373.6)))
  }

  "DataStream.addField" should "support addition expressions" in {
    import io.eels._
    ds.filter(select("album") === "Lionheart")
      .addField(Field("woo", DoubleType), select("year") + 1.2)
      .collectValues shouldBe Vector(Vector("Kate Bush", 1978, "Lionheart", 745, BigDecimal(1979.2)))
  }

  "DataStream.addField" should "support subtraction expressions" in {
    import io.eels._
    ds.filter(select("album") === "Lionheart")
      .addField(Field("woo", DoubleType), select("year") - 1.2)
      .collectValues shouldBe Vector(Vector("Kate Bush", 1978, "Lionheart", 745, BigDecimal(1976.8)))
  }

  "DataStream.addField" should "support division expressions" in {
    import io.eels._
    ds.filter(select("album") === "Lionheart")
      .addField(Field("woo", DoubleType), select("year") / 2)
      .collectValues shouldBe Vector(Vector("Kate Bush", 1978, "Lionheart", 745, 989))
  }
}
