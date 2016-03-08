package io.eels.component.jdbc

import java.sql.DriverManager

import io.eels.{Column, Schema, SchemaType}
import org.scalatest.{Matchers, WordSpec}

class JdbcSourceTest extends WordSpec with Matchers {

  import scala.concurrent.ExecutionContext.Implicits.global

  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:mem:test")
  conn.createStatement().executeUpdate("create table mytable (a integer, b bit, c bigint)")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")

  "JdbcSource" should {
    "read schema" in {
      JdbcSource("jdbc:h2:mem:test", "select * from mytable").schema shouldBe {
        Schema(List(
          Column("A", SchemaType.Int, true, 10),
          Column("B", SchemaType.Boolean, true, 1),
          Column("C", SchemaType.Long, true, 19)
        ))
      }
    }
    "read from jdbc" in {
      JdbcSource("jdbc:h2:mem:test", "select * from mytable").size shouldBe 2
    }
    "use supplied query" in {
      JdbcSource("jdbc:h2:mem:test", "select * from mytable where a=4").size shouldBe 1
    }
  }
}
