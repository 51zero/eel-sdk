package io.eels.component.parquet

import java.util.function.Consumer

import com.sksamuel.exts.metrics.Timed
import io.eels.Row
import io.eels.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import reactor.core.publisher.{Flux, FluxSink}
import reactor.core.scheduler.Schedulers

import scala.io.Source
import scala.util.Random

// test different strategies of reading a file into rows
object ParquetReadStrategiesTest extends App with Timed {

  implicit val conf = new Configuration()
  implicit val fs = FileSystem.getLocal(new Configuration)
  ParquetLogMute()

  val text = getClass.getResourceAsStream("/huckleberry_finn.txt")
  val string = Source.fromInputStream(text).mkString

  val schema = StructType(
    Field("word", StringType),
    Field("count", IntType.Signed)
  )

  private val path = new Path("parquet_single")
  if (!fs.exists(path)) {
    val rows = (string + string + string + string + string.reverse + string.reverse + string + string.reverse + string.reverse + string + string + string.reverse + string.reverse + string + string.reverse + string.reverse).split(' ').map { word =>
      Row(schema, Vector(word, Random.nextInt(10000)))
    }.toSeq

    ParquetSink(path).write(rows)
    println(s"Written $path")
  }

  while (true) {
    timed("scala iterator") {
      val reader = ParquetReaderFn(path, None, None)
      val iter = ParquetRowIterator(reader)
      val count = iter.foldLeft(0L)((counter, row) => counter + 1)
      println(count)
    }

    timed("using a flux") {
      val flux = Flux.create(new Consumer[FluxSink[Row]] {
        override def accept(sink: FluxSink[Row]): Unit = {
          println("Consumer thread=" + Thread.currentThread)
          val reader = ParquetReaderFn(path, None, None)
          val iter = ParquetRowIterator(reader)
          while (!sink.isCancelled && iter.hasNext) {
            //if (System.currentTimeMillis % 10 == 0)
            //  println("Consumer thread=" + Thread.currentThread)
            sink.next(iter.next)
          }
          sink.complete()
          reader.close()
        }
      }, FluxSink.OverflowStrategy.BUFFER)
      val count = flux.parallel().runOn(Schedulers.parallel())
        .doOnNext(new Consumer[Row] {
          override def accept(t: Row): Unit = println("I'm on thread " + Thread.currentThread() + " " + t)
        })
        .sequential().count().block()
      println("Count thread=" + Thread.currentThread)
      println(count)
    }
  }

}