package com.sksamuel.hs

import java.sql.DriverManager

import org.scalatest.{WordSpec, Matchers}

class JdbcTest extends WordSpec with Matchers {

  import HadoopDsl._

  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:mem:test")
  conn.createStatement().executeUpdate("create table mytable (a integer, b integer, c integer)")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
  conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")

  "JdbcSource" should {
    "read from jdbc" in {
      JdbcSource("jdbc:h2:mem:test", "select * from mytable").size shouldBe 2
    }
  }
}
