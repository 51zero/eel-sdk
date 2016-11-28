package io.eels
import java.nio.file.Paths
import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.eels.component.csv.{CsvSink, CsvSource}
import io.eels.schema.StructType
import org.scalatest.{Matchers, WordSpec}

import scala.util.Random

class ListenerTest extends WordSpec with Matchers {

  val schema = StructType("a", "b", "c", "d", "e")
  val rows = List.fill(1000)(Row(schema, Random.nextBoolean(), Random.nextFloat(), Random.nextGaussian(), Random.nextLong(), Random.nextString(10)))
  val frame = Frame(schema, rows)

  "Frame.to" should {
    "support user's listeners" in {
      val latch = new CountDownLatch(1000)

      val path = Paths.get("csv_speed.csv")
      path.toFile.delete()

      frame.to(CsvSink(path), new Listener {
        override def onNext(value: Row): Unit = latch.countDown()
        override def onError(e: Throwable): Unit = ()
        override def onComplete(): Unit = ()
      })

      latch.await(20, TimeUnit.SECONDS) shouldBe true
      path.toFile.delete()
    }
  }

  "Source.toFrame" should {
    "support user's observers" in {

      val path = Paths.get("csv_speed.csv")
      path.toFile.delete()
      frame.to(CsvSink(path))

      val latch = new CountDownLatch(1000)

      CsvSource(path).toFrame(new Listener {
        override def onNext(value: Row): Unit = latch.countDown()
        override def onError(e: Throwable): Unit = ()
        override def onComplete(): Unit = ()
      }).toList()

      latch.await(20, TimeUnit.SECONDS) shouldBe true
      path.toFile.delete()
    }
  }
}
