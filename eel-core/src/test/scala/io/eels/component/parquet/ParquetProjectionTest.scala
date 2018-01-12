package io.eels.component.parquet

import java.io.{File, FilenameFilter}

import io.eels.datastream.DataStream
import io.eels.schema.{Field, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{FlatSpec, Matchers}

class ParquetProjectionTest extends FlatSpec with Matchers {

  cleanUpResidualParquetTestFiles

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
  private val file = new File(s"test_${System.currentTimeMillis()}.pq")
  file.deleteOnExit()
  private val path = new Path(file.toURI)

  if (fs.exists(path))
    fs.delete(path, false)

  ds.to(ParquetSink(path).withOverwrite(true))

  "ParquetSource" should "support projections" in {
    val rows = ParquetSource(path).withProjection("name").toDataStream().collect
    rows.map(_.values) shouldBe Vector(Vector("clint eastwood"), Vector("elton john"))
  }

  it should "return all data when no projection is set" in {
    val rows = ParquetSource(path).toDataStream().collect
    rows.map(_.values) shouldBe Vector(Vector("clint eastwood", "actor", "carmel"), Vector("elton john", "musician", "pinner"))
  }

  private def cleanUpResidualParquetTestFiles = {
    new File(".").listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        (name.startsWith("test_") && name.endsWith(".pq")) || (name.startsWith(".test_") && name.endsWith(".pq.crc"))
      }
    }).foreach(_.delete())
  }

}
