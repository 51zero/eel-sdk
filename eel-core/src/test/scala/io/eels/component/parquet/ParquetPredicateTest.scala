package io.eels.component.parquet

import java.io.File
import java.sql.Timestamp

import io.eels.datastream.DataStream
import io.eels.schema._
import io.eels.{Predicate, Row}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

class ParquetPredicateTest extends FlatSpec with Matchers {

  private val schema = StructType(
    Field("name", StringType, nullable = false),
    Field("job", StringType, nullable = false),
    Field("location", StringType, nullable = false)
  )

  private val ds = DataStream.fromValues(
    schema,
    Seq(
      Vector("clint eastwood", "actor", "carmel"),
      Vector("elton john", "musician", "pinner")
    )
  )

  private implicit val conf = new Configuration()
  private implicit val fs = FileSystem.get(new Configuration())
  private val path = new Path("test.pq")

  if (fs.exists(path))
    fs.delete(path, false)

  new File(path.toString).deleteOnExit()

  ds.to(ParquetSink(path))

  "ParquetSource" should "support predicates" in {
    val rows = ParquetSource(path).withPredicate(Predicate.equals("job", "actor")).toDataStream().collect
    rows.size shouldBe 1
  }

  it should "support timestamp predicates" ignore {

    val schema = StructType(
      Field("id", StringType),
      Field("timestamp", TimestampMillisType)
    )

    val ds = DataStream.fromValues(schema, Seq(
      Vector("a", Timestamp.valueOf("2017-05-01 12:12:12")),
      Vector("b", Timestamp.valueOf("2017-06-02 11:11:11")),
      Vector("c", Timestamp.valueOf("2017-07-03 10:10:10"))
    ))

    val path = new Path("timestamp_predicate.pq")
    if (fs.exists(path))
      fs.delete(path, false)

    ds.to(ParquetSink(path))

    ParquetSource(path).withPredicate(Predicate.gte("timestamp", Timestamp.valueOf("2017-06-02 11:11:11"))).toDataStream().collect shouldBe
      Seq(
         Vector("a", Timestamp.valueOf("2017-05-01 12:12:12")),
         Vector("b", Timestamp.valueOf("2017-06-02 11:11:11")),
         Vector("c", Timestamp.valueOf("2017-07-03 10:10:10"))
      )

    fs.delete(path, false)
  }

  it should "support decimal type predicate via user defined predicate" ignore {

    val schema = StructType(
      Field("ticker", StringType, nullable = false),
      Field("price", DecimalType(10, 2), nullable = false)
    )
    val ds = DataStream.fromValues(schema, Seq(Vector("goog", BigDecimal(100.52)), Vector("tsla", BigDecimal(19.13))))

    val path = new Path("decimaltest.pq")
    if (fs.exists(path))
      fs.delete(path, false)

    ds.to(ParquetSink(path))

    val source = ParquetSource(path)
      .withDictionaryFiltering(false)

    source.withPredicate(Predicate.gt("price", BigInt(5500)))
      .toDataStream().collect shouldBe Vector("goog")

    source.withPredicate(Predicate.lte("price", BigInt(10051)))
      .toDataStream().collect shouldBe Vector("tsla")

    source.withPredicate(Predicate.lte("price", BigInt(10052)))
      .toDataStream().collect shouldBe Vector("goog", "tsla")

    source.withPredicate(Predicate.lt("price", BigInt(10052)))
      .toDataStream().collect shouldBe Vector("tsla")

    source.withPredicate(Predicate.gte("price", BigInt(1913)))
      .toDataStream().collect shouldBe Vector("goog", "tsla")

    source.withPredicate(Predicate.gte("price", BigInt(1914)))
      .toDataStream().collect shouldBe Vector("goog")

    source.withPredicate(Predicate.gt("price", BigInt(1913)))
      .toDataStream().collect shouldBe Vector("goog")

    source.withPredicate(Predicate.equals("price", BigInt(1913)))
      .toDataStream().collect shouldBe Vector("tsla")

    source.withPredicate(Predicate.equals("price", BigInt(10052)))
      .toDataStream().collect shouldBe Vector("goog")

    source.withPredicate(Predicate.notEquals("price", BigInt(10052)))
      .toDataStream().collect shouldBe Vector("tsla")

    fs.delete(path, false)
  }
}
