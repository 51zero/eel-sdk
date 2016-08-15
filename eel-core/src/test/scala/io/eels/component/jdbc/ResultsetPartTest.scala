package io.eels.component.jdbc

import java.sql.DriverManager

import io.eels.NoopRowListener
import io.eels.schema.{Field, FieldType, Precision, Schema}
import org.scalatest.{Matchers, WordSpec}

class ResultsetPartTest extends WordSpec with Matchers {

  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:mem:ResultsetPartTest")
  conn.createStatement().executeUpdate("create table mytable (a integer, b bit, c bigint)")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")

  "ResultsetPart" should {
    "publish fields in schema order" in {

      val schema = Schema(
        Field("c", FieldType.Int, true, Precision(10), signed = true),
        Field("b", FieldType.Boolean, true, Precision(1), signed = true),
        Field("a", FieldType.Long, true, Precision(19), signed = true)
      )

      val stmt = conn.createStatement()
      val rs = stmt.executeQuery("select * from mytable")
      val data = new ResultsetPart(rs, stmt, conn, schema, NoopRowListener).data().toBlocking.head
      data.values shouldBe Vector(3L, true, 1)
    }
  }
}
