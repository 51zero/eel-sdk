package io.eels.component.jdbc

import java.sql.DriverManager
import java.util.concurrent.{CountDownLatch, TimeUnit}

import io.eels.schema.{Field, Schema}
import io.eels.{Frame, Row, RowListener}
import org.scalatest.{Matchers, WordSpec}

class JdbcSinkTest extends WordSpec with Matchers {

  Class.forName("org.h2.Driver")
  val conn = DriverManager.getConnection("jdbc:h2:mem:test")
  conn.createStatement().executeUpdate("create table mytab (a integer, b integer, c integer)")

  val schema = Schema(Field("a"), Field("b"), Field("c"))
  val frame = io.eels.Frame(schema, Row(schema, Vector("1", "2", "3")), Row(schema, Vector("4", "5", "6")))

  "JdbcSink" should {
    "write frame to table" in {
      frame.to(JdbcSink("jdbc:h2:mem:test", "mytab"))
      val rs = conn.createStatement().executeQuery("select count(*) from mytab")
      rs.next()
      rs.getLong(1) shouldBe 2L
      rs.close()
    }
    "createReader table if createTable is true" in {
      frame.to(JdbcSink("jdbc:h2:mem:test", "qwerty", createTable = true))
      val rs = conn.createStatement().executeQuery("select count(*) from qwerty")
      rs.next()
      rs.getLong(1) shouldBe 2L
      rs.close()
    }
    "support multiple writers" in {
      val rows = List.fill(10000)(Row(schema, Vector("1", "2", "3")))
      val mframe = Frame(schema, rows)
      val sink = JdbcSink("jdbc:h2:mem:test", "multithreads", createTable = true, threads = 4)
      mframe.to(sink)
      val rs = conn.createStatement().executeQuery("select count(*) from multithreads")
      rs.next()
      rs.getLong(1) shouldBe 10000L
      rs.close()
    }
    "support row callbacks" in {
      val latch = new CountDownLatch(2)
      frame.to(JdbcSink("jdbc:h2:mem:test", "callymccallface", createTable = true, listener = new RowListener {
        override def onRow(row: Row) {
          latch.countDown()
        }
      }))
      latch.await(10, TimeUnit.SECONDS) shouldBe true
    }
  }
}
