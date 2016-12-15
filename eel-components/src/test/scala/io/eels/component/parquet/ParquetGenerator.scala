package io.eels.component.parquet

import com.sksamuel.exts.OptionImplicits._
import io.eels.Row
import io.eels.component.parquet.avro.AvroParquetSink
import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source
import scala.util.Random

object ParquetGenerator extends App {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration)

  val text = getClass.getResourceAsStream("/wisdom_of_fools.txt")
  val string = Source.fromInputStream(text).mkString

  val schema = StructType(
    Field("word", StringType),
    Field("doubles", ArrayType(DoubleType))
  )

  for (k <- 1 to 200) {
    val rows = string.split(' ').distinct.map { word =>
      Row(schema, Vector(word, List.fill(50)(Random.nextDouble)))
    }.toSeq

    val path = new Path("wisdom_of_fools.parquet")
    fs.delete(path, false)
    AvroParquetSink(path).write(rows)
  }

}
