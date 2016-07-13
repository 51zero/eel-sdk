package io.eels.component.jdbc

import io.eels.schema.Field
import io.eels.schema.FieldType
import io.eels.schema.Precision
import io.eels.schema.Schema
import io.kotlintest.specs.WordSpec
import java.sql.DriverManager

class JdbcSourceTest : WordSpec() {

  init {

    Class.forName("org.h2.Driver")
    val conn = DriverManager.getConnection("jdbc:h2:mem:test")
    conn.createStatement().executeUpdate("create table mytable (a integer, b bit, c bigint)")
    conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
    conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")

    "JdbcSource" should {
      "read schema"  {
        JdbcSource("jdbc:h2:mem:test", "select * from mytable").schema() shouldBe
            Schema(
                Field("A", FieldType.Int, true, Precision(10), signed = true),
                Field("B", FieldType.Boolean, true, Precision(1), signed = true),
                Field("C", FieldType.Long, true, Precision(19), signed = true)
            )
      }
      "read from jdbc"  {
        JdbcSource("jdbc:h2:mem:test", "select * from mytable").toFrame(1).size() shouldBe 2
      }
      "use supplied query"  {
        JdbcSource("jdbc:h2:mem:test", "select * from mytable where a=4").toFrame(1).size() shouldBe 1
        val a = JdbcSource("jdbc:h2:mem:test", "select a,c from mytable where a=4").toFrame(1).toList()
        a.first().values.first() shouldBe 4
        a.first().values.get(1) shouldBe 6L
      }
    }
  }
}
