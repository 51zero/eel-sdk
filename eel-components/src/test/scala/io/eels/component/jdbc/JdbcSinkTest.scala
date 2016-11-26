package io.eels.component.jdbc

import java.sql.DriverManager
import java.util.UUID

import io.eels.schema.{Field, Schema}
import io.eels.{Frame, Row}
import org.scalatest.{Matchers, OneInstancePerTest, WordSpec}

class JdbcSinkTest extends WordSpec with Matchers with OneInstancePerTest {

  Class.forName("org.h2.Driver")
  val url = "jdbc:h2:mem:" + UUID.randomUUID.toString.replace("-", "")
  val conn = DriverManager.getConnection(url)
  conn.createStatement().executeUpdate("create table mytab (a integer, b integer, c integer)")

  val schema = Schema(Field("a"), Field("b"), Field("c"))
  val frame = io.eels.Frame(schema, Row(schema, Vector("1", "2", "3")), Row(schema, Vector("4", "5", "6")))

  "JdbcSink" should {
    "write frame to table" in {
      frame.to(JdbcSink(url, "mytab"))
      val rs = conn.createStatement().executeQuery("select count(*) from mytab")
      rs.next()
      rs.getLong(1) shouldBe 2L
      rs.close()
    }
    "create table if createTable is true" in {
      frame.to(JdbcSink(url, "qwerty").withCreateTable(true))
      val rs = conn.createStatement().executeQuery("select count(*) from qwerty")
      rs.next()
      rs.getLong(1) shouldBe 2L
      rs.close()
    }
    "support multiple writers" in {
      val rows = List.fill(10000)(Row(schema, Vector("1", "2", "3")))
      val mframe = Frame(schema, rows)
      val sink = JdbcSink(url, "multithreads").withCreateTable(true).withThreads(4)
      mframe.to(sink)
      val rs = conn.createStatement().executeQuery("select count(*) from multithreads")
      rs.next()
      rs.getLong(1) shouldBe 10000L
      rs.close()
    }
  }
}
