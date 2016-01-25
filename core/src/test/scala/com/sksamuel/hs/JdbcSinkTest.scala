package com.sksamuel.hs

import java.sql.DriverManager

import com.sksamuel.hs.sink.JdbcSink
import org.scalatest.{Matchers, WordSpec}

class JdbcSinkTest extends WordSpec with Matchers {

  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:mem:test")
  conn.createStatement().executeUpdate("create table mytab (d integer, e integer, f integer)")

  "JdbcSource" should {
    "write frame to table" in {
      val frame = Frame.fromSeq(Seq(Seq("1", "2", "3"), Seq("4", "5", "6"), Seq("7", "8", "9")))
      frame.to(JdbcSink("jdbc:h2:mem:test", "mytab"))
      val rs = conn.createStatement().executeQuery("select count(*) from mytab")
      rs.next
      rs.getLong(1) shouldBe 3
    }
  }
}
