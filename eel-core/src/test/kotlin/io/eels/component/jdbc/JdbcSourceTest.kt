package io.eels.component.jdbc

import io.eels.Row
import io.eels.RowListener
import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Precision
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class JdbcSourceTest : WordSpec() {

  init {

    Class.forName("org.h2.Driver")

    "JdbcSource" should {
      "read schema"  {
        val conn = DriverManager.getConnection("jdbc:h2:mem:test")
        conn.createStatement().executeUpdate("createReader table mytable (a integer, b bit, c bigint)")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")
        JdbcSource("jdbc:h2:mem:test", "select * from mytable").schema() shouldBe
            Schema(
                Field("A", FieldType.Int, true, Precision(10), signed = true),
                Field("B", FieldType.Boolean, true, Precision(1), signed = true),
                Field("C", FieldType.Long, true, Precision(19), signed = true)
            )
      }
      "trigger callbacks per row" {
        val latch = CountDownLatch(2)
        val conn = DriverManager.getConnection("jdbc:h2:mem:test2")
        conn.createStatement().executeUpdate("createReader table mytable (a integer, b bit, c bigint)")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")
        JdbcSource("jdbc:h2:mem:test2", "select * from mytable").withListener(object : RowListener {
          override fun onRow(row: Row) {
            latch.countDown()
          }
        }).toFrame(1).size()
        latch.await(15, TimeUnit.SECONDS) shouldBe true
      }
      "use supplied query"  {
        val conn = DriverManager.getConnection("jdbc:h2:mem:test3")
        conn.createStatement().executeUpdate("createReader table mytable (a integer, b bit, c bigint)")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")
        JdbcSource({
          DriverManager.getConnection("jdbc:h2:mem:test3")
        }, "select * from mytable where a=4").toFrame(1).size() shouldBe 1
        val a = JdbcSource("jdbc:h2:mem:test3", "select a,c from mytable where a=4").toFrame(1).toList()
        a.first().values.first() shouldBe 4
        a.first().values.get(1) shouldBe 6L
      }
      "read from jdbc"  {
        val conn = DriverManager.getConnection("jdbc:h2:mem:test4")
        conn.createStatement().executeUpdate("createReader table mytable (a integer, b bit, c bigint)")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")
        JdbcSource("jdbc:h2:mem:test4", "select * from mytable").toFrame(1).size() shouldBe 2
      }
      "support bind" {
        val conn = DriverManager.getConnection("jdbc:h2:mem:test5")
        conn.createStatement().executeUpdate("createReader table mytable (a integer, b bit, c bigint)")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
        conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")
        JdbcSource("jdbc:h2:mem:test5", "select * from mytable where a=?").withBind { it.setLong(1, 4) }.toFrame(1).size() shouldBe 1
      }
    }
  }
}
