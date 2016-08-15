package io.eels

import com.sksamuel.exts.io.IO
import io.eels.component.csv.CsvSource
import io.eels.schema.{Field, FieldType, Precision, Schema}
import org.scalatest.{Matchers, WordSpec}

class SqlContextTest extends WordSpec with Matchers {

  "SqlContext" should {
    "accept simple queries" ignore {
      val frame = CsvSource(IO.pathFromResource("/us-500.csv")).toFrame(1)
      val sqlContext = new SqlContext()
      sqlContext.registerFrame("people", frame)
      val result = sqlContext.sql("select first_name, last_name from people ")
      result.schema shouldBe Schema(
        Field("FIRST_NAME", FieldType.String, true, precision = Precision(255), signed = true),
        Field("LAST_NAME", FieldType.String, true, precision = Precision(255), signed = true)
      )
      result.size shouldBe 500
    }
    "accept group by queries" ignore {
      val frame = CsvSource(IO.pathFromResource("/us-500.csv")).toFrame(1)
      val sqlContext = new SqlContext()
      sqlContext.registerFrame("people", frame)
      val result = sqlContext.sql("select state, count(*) from people group by state")
      result.schema shouldBe Schema(
        Field("STATE", FieldType.String, true, precision = Precision(255), signed = true),
        Field("COUNT(*)", FieldType.Long, true, Precision(19), signed = true)
      )
      result.size shouldBe 47
    }
  }
}
