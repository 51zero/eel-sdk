package io.eels

import com.sksamuel.scalax.io.IO
import io.eels.component.csv.CsvSource
import org.scalatest.{Matchers, WordSpec}

class SqlContextTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  "SqlContext" should {
    "accept simple queries" in {
      val frame = CsvSource(IO.pathFromResource("/us-500.csv"))
      val sqlContext = SqlContext()
      sqlContext.registerFrame("people", frame)
      val result = sqlContext.sql("select first_name, last_name from people ")
      result.schema shouldBe Schema(
        List(
          Column("FIRST_NAME", SchemaType.String, true, precision = 255, signed = true),
          Column("LAST_NAME", SchemaType.String, true, precision = 255, signed = true)
        )
      )
      result.size shouldBe 500
    }
    "accept group by queries" in {
      val frame = CsvSource(IO.pathFromResource("/us-500.csv"))
      val sqlContext = SqlContext()
      sqlContext.registerFrame("people", frame)
      val result = sqlContext.sql("select state, count(*) from people group by state")
      result.schema shouldBe Schema(List(
        Column("STATE", SchemaType.String, true, precision = 255, signed = true),
        Column("COUNT(*)", SchemaType.Long, true, 19, signed = true)
      ))
      result.size shouldBe 47
    }
  }
}
