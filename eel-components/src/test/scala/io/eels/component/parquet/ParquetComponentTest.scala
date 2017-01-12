package io.eels.component.parquet

import java.io.File

import io.eels.{Frame, Row}
import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

class ParquetComponentTest extends FlatSpec with Matchers {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.get(new Configuration())

  "Parquet" should "support nested structs" in {

    val structType = StructType(
      Field("name", StringType),
      Field("homeworld", StructType(
        Field("name", StringType),
        Field("x", IntType.Signed),
        Field("y", IntType.Signed),
        Field("z", IntType.Signed)
      ))
    )

    val frame = Frame.fromValues(
      structType,
      Vector("federation", Vector("sol", 0, 0, 0)),
      Vector("empire", Vector("andromeda", 914, 735, 132))
    )

    val path = new Path("test.pq")
    if (fs.exists(path))
      fs.delete(path, false)
    new File(path.toString).deleteOnExit()

    frame.to(ParquetSink(path))

    val rows = ParquetSource(path).toFrame().collect()
    rows shouldBe Seq(
      Row(structType, Vector("federation", Vector("sol", 0, 0, 0))),
      Row(structType, Vector("empire", Vector("andromeda", 914, 735, 132)))
    )
  }
}
