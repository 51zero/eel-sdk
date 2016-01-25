package com.sksamuel.hs

import java.sql.DriverManager

import com.sksamuel.hs.source.JdbcSource
import org.scalatest.{WordSpec, Matchers}

class JdbcSourceTest extends WordSpec with Matchers {

  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:mem:test")
  conn.createStatement().executeUpdate("create table mytable (a integer, b integer, c integer)")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")

  "JdbcSource" should {
    "read from jdbc" in {
      JdbcSource("jdbc:h2:mem:test", "select * from mytable").size shouldBe 2
    }
    "use supplied query" in {
      JdbcSource("jdbc:h2:mem:test", "select * from mytable where a=4").size shouldBe 1
    }
  }
}
