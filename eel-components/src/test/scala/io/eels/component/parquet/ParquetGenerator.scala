package io.eels.component.parquet

import com.sksamuel.exts.OptionImplicits._
import io.eels.Row
import io.eels.schema.{Field, FieldType, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source
import scala.util.Random

object ParquetGenerator extends App {

  implicit val fs = FileSystem.getLocal(new Configuration)

  val text = getClass.getResourceAsStream("/wisdom_of_fools.txt")
  val string = Source.fromInputStream(text).mkString

  val schema = Schema(
    Field("word", FieldType.String),
    Field("doubles", FieldType.Array, arrayType = FieldType.Double.some)
  )

  val rows = string.split(' ').distinct.map { word =>
    Row(schema, Vector(word, List.fill(50)(Random.nextDouble)))
  }.toSeq

  val path = new Path("wisdom_of_fools.parquet")
  fs.delete(path, false)
  ParquetSink(path).write(rows)

}
