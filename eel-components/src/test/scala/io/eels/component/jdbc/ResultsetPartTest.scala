package io.eels.component.jdbc

import java.sql.DriverManager

import io.eels.schema._
import org.scalatest.{Matchers, WordSpec}

class ResultsetPartTest extends WordSpec with Matchers {

  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:mem:ResultsetPartTest")
  conn.createStatement().executeUpdate("create table mytable (a integer, b bit, c bigint)")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")

  "ResultsetPart" should {
    "publish fields in schema order" in {

      val schema = StructType(
        Field("c", IntType(true)),
        Field("b", BooleanType),
        Field("a", LongType(true))
      )

      val stmt = conn.createStatement()
      val rs = stmt.executeQuery("select * from mytable")
      val data = new ResultsetPart(rs, stmt, conn, schema).data().blockingFirst()
      data.values shouldBe Vector(3L, true, 1)
    }
  }
}
