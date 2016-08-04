package io.eels.component.jdbc

import io.eels.Row
import io.eels.RowListener
import io.eels.schema.Field
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class JdbcSinkTest : WordSpec() {

  init {

    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection("jdbc:h2:mem:test")
    conn.createStatement().executeUpdate("createReader table mytab (a integer, b integer, c integer)")

    val schema = Schema(Field("a"), Field("b"), Field("c"))
    val frame = io.eels.Frame(schema, Row(schema, listOf("1", "2", "3")), Row(schema, listOf("4", "5", "6")))

    "JdbcSink" should {
      "write frame to table" {
        frame.to(JdbcSink("jdbc:h2:mem:test", "mytab"))
        val rs = conn.createStatement().executeQuery("select count(*) from mytab")
        rs.next()
        rs.getLong(1) shouldBe 2L
        rs.close()
      }
      "createReader table if createTable is true"  {
        frame.to(JdbcSink("jdbc:h2:mem:test", "qwerty", createTable = true))
        val rs = conn.createStatement().executeQuery("select count(*) from qwerty")
        rs.next()
        rs.getLong(1) shouldBe 2L
        rs.close()
      }
      "support multiple writers" {
        val rows = Array(100000, { Row(schema, listOf("1", "2", "3")) }).asList()
        val mframe = io.eels.Frame(schema, rows)
        val sink = JdbcSink("jdbc:h2:mem:test", "multithreads", createTable = true, threads = 4)
        mframe.to(sink)
        val rs = conn.createStatement().executeQuery("select count(*) from multithreads")
        rs.next()
        rs.getLong(1) shouldBe 100000L
        rs.close()
      }
      "support row callbacks" {
        val latch = CountDownLatch(2)
        frame.to(JdbcSink("jdbc:h2:mem:test", "callymccallface", createTable = true, listener = object : RowListener {
          override fun onRow(row: Row) {
            latch.countDown()
          }
        }))
        latch.await(10, TimeUnit.SECONDS) shouldBe true
      }
    }
  }
}
