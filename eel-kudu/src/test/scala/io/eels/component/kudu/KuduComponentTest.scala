package io.eels.component.kudu

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema._
import org.scalatest.{FlatSpec, Matchers, Tag}

object Kudu extends Tag("kudu")

class KuduComponentTest extends FlatSpec with Matchers {

  "kudu" should "support end to end sink to source" taggedAs Kudu in {

    val schema = StructType(
      Field("planet", StringType, nullable = false, key = true),
      Field("position", StringType, nullable = true)
    )

    val ds = DataStream.fromValues(
      schema,
      Seq(
        Vector("earth", 3),
        Vector("saturn", 6)
      )
    )

    val master = "localhost:7051"
    ds.to(KuduSink(master, "mytable"))

    val rows = KuduSource(master, "mytable").toDataStream().collect
    rows shouldBe Seq(
      Row(schema, Vector("earth", "3")),
      Row(schema, Vector("saturn", "6"))
    )
  }

  it should "support all basic types" taggedAs Kudu in {

    val schema = StructType(
      Field("planet", StringType, nullable = false, key = true),
      Field("position", ByteType.Signed, nullable = false),
      Field("volume", DoubleType, nullable = false),
      Field("bytes", BinaryType, nullable = false),
      Field("gas", BooleanType, nullable = false),
      Field("distance", LongType.Signed, nullable = false)
    )

    val data = Array("earth", 3: Byte, 4515135988.632, Array[Byte](1, 2, 3), false, 83000000)

    val ds = DataStream.fromValues(schema, Seq(data))

    val master = "localhost:7051"
    ds.to(KuduSink(master, "mytable2"))

    val rows = KuduSource(master, "mytable2").toDataStream().collect
    val values = rows.head.values.toArray
    data(3) = data(3).asInstanceOf[Array[Byte]].toList
    values shouldBe data
  }
}
