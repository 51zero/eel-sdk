package io.eels.component.csv

import java.util.concurrent.{Executors, TimeUnit}

import com.sksamuel.exts.Logging
import org.apache.commons.io.IOUtils
import org.scalatest.{FunSuite, Matchers}

class CsvTest extends FunSuite with Matchers with Logging {

  test("concurrent loading") {
    val executor = Executors.newFixedThreadPool(20)
    for (k <- 1 to 1000) {
      executor.execute(new Runnable {
        override def run() = {
          try {
            val data = IOUtils.toByteArray(getClass.getResourceAsStream("/uk-500.csv"))
            val source = CsvSource(data)
            val rows = source.toDataStream.collect
            require(rows.size == 500, s"Rows should be 500 but is ${rows.size}")
          } catch {
            case e: Exception =>
              logger.error("oops", e)
          }
        }
      })
    }
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS)
  }
}
