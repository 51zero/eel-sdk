package io.eels.component.hive

import java.io.File
import java.math.MathContext

import com.sksamuel.exts.metrics.Timed
import io.eels.Row
import io.eels.component.orc.{OrcSink, OrcSource}
import io.eels.component.parquet.{ParquetSink, ParquetSource}
import io.eels.datastream.DataStream
import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.math.BigDecimal.RoundingMode
import scala.util.Random

object ParquetVsOrcSpeedTest extends App with Timed {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration())

  val size = 5000000

  val structType = StructType(
    Field("name", StringType),
    Field("age", IntType.Signed),
    Field("height", DoubleType),
    Field("amazing", BooleanType),
    Field("fans", LongType.Signed),
    Field("rating", DecimalType(4, 2))
  )

  def iter: Iterator[Vector[Any]] = Iterator.continually(Vector(
    Random.nextString(10),
    Random.nextInt(),
    Random.nextDouble(),
    Random.nextBoolean(),
    Random.nextLong(),
    BigDecimal(Random.nextDouble(), new MathContext(4)).setScale(2, RoundingMode.UP)
  ))

  def ds: DataStream = DataStream.fromIterator(structType, iter.take(size).map(Row(structType, _)))

  val ppath = new Path("parquet_speed.pq")
  fs.delete(ppath, false)

  val opath = new Path("orc_speed.orc")
  fs.delete(opath, false)

  new File(ppath.toString).deleteOnExit()
  new File(opath.toString).deleteOnExit()

  timed("Orc Insertion") {
    ds.to(OrcSink(opath))
  }

  timed("Parquet Insertion") {
    ds.to(ParquetSink(ppath))
  }

  while (true) {

    timed("Reading with OrcSource") {
      val actual = OrcSource(opath).toDataStream().size
      assert(actual == size, s"$actual != $size")
    }

    timed("Reading with ParquetSource") {
      val actual = ParquetSource(ppath).toDataStream().size
      assert(actual == size, s"$actual != $size")
    }
  }
}
