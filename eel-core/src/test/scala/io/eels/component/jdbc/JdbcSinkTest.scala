package io.eels.component.jdbc

import java.sql.DriverManager

import io.eels.{Column, Frame, Row}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class JdbcSinkTest extends WordSpec with Matchers with BeforeAndAfterAll {

  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:mem:test")
  conn.createStatement().executeUpdate("create table mytab (a integer, b integer, c integer)")

  val columns = List(Column("a"), Column("b"), Column("c"))
  def frame: Frame = Frame(
    Row(columns, List("1", "2", "3")),
    Row(columns, List("4", "5", "6")),
    Row(columns, List("7", "8", "9"))
  )

  override protected def afterAll(): Unit = {
    conn.close()
  }

  "JdbcSink" should {
    "write frame to table" in {
      frame.to(JdbcSink("jdbc:h2:mem:test", "mytab")).run
        val rs = conn.createStatement().executeQuery("select count(*) from mytab")
        rs.next
        rs.getLong(1) shouldBe 3
        rs.close()
    }
    "create table" in {
      frame.to(JdbcSink("jdbc:h2:mem:test", "qwerty", JdbcSinkProps(createTable = true))).run
        val rs = conn.createStatement().executeQuery("select count(*) from qwerty")
        rs.next
        rs.getLong(1) shouldBe 3
        rs.close()
    }
    "support multiple writers" in {
      frame.to(JdbcSink("jdbc:h2:mem:test", "multithreads", JdbcSinkProps(createTable = true, threads = 3))).run
        val rs = conn.createStatement().executeQuery("select count(*) from qwerty")
        rs.next
        rs.getLong(1) shouldBe 3
        rs.close()
    }
  }
}
