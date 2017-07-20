package io.eels.component.avro

import io.eels.Row
import io.eels.datastream.DataStream
import io.eels.schema.{ArrayType, Field, MapType, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, WordSpec}

class AvroSinkTest extends WordSpec with Matchers {

  private implicit val conf = new Configuration()
  private implicit val fs = FileSystem.get(new Configuration())

  private val ds = DataStream.fromValues(
    StructType("name", "job", "location"),
    Seq(
      List("clint eastwood", "actor", "carmel"),
      List("elton john", "musician", "pinner"),
      List("issac newton", "scientist", "heaven")
    )
  )

  "AvroSink" should {
    "write to avro" in {
      val path = new Path("avro.test")
      fs.delete(path, false)
      ds.to(AvroSink(path))
      fs.delete(path, false)
    }
    "support overwrite option" in {
      val path = new Path("overwrite_test", ".avro")
      fs.delete(path, false)
      ds.to(AvroSink(path))
      ds.to(AvroSink(path).withOverwrite(true))
      fs.delete(path, false)
    }
    "write lists and maps" in {
      val ds = DataStream.fromValues(
        StructType(
          Field("name"),
          Field("movies", ArrayType(StringType)),
          Field("characters", MapType(StringType, StringType))
        ),
        Seq(
          List(
            "clint eastwood",
            List("fistful of dollars", "high plains drifters"),
            Map("preacher" -> "high plains", "no name" -> "good bad ugly")
          )
        )
      )

      val path = new Path("array_map_avro", ".avro")
      fs.delete(path, false)
      ds.to(AvroSink(path))
      AvroSource(path).toDataStream().collect shouldBe Seq(
        Row(
          ds.schema,
          Seq(
            "clint eastwood",
            List("fistful of dollars", "high plains drifters"),
            Map("preacher" -> "high plains", "no name" -> "good bad ugly")
          )
        )
      )

      fs.delete(path, true)
    }
  }
}