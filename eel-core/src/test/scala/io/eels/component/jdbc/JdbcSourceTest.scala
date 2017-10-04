package io.eels.component.jdbc

import java.sql.DriverManager

import io.eels.schema._
import org.scalatest.{Matchers, WordSpec}

class JdbcSourceTest extends WordSpec with Matchers {

  Class.forName("org.h2.Driver")

  "JdbcSource" should {
    "read schema" in {
      val conn = DriverManager.getConnection("jdbc:h2:mem:test")
      conn.createStatement().executeUpdate("create table mytable (a integer, b bit, c bigint)")
      conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
      conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")
      JdbcSource("jdbc:h2:mem:test", "select * from mytable").schema shouldBe
        StructType(
          Field("A", IntType(true), true),
          Field("B", BooleanType, true),
          Field("C", LongType.Signed, true)
        )
    }
    "use supplied query" in {
      val conn = DriverManager.getConnection("jdbc:h2:mem:test3")
      conn.createStatement().executeUpdate("create table mytable (a integer, b bit, c bigint)")
      conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
      conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")
      JdbcSource(() => DriverManager.getConnection("jdbc:h2:mem:test3"), "select * from mytable where a=4").toDataStream().size shouldBe 1
      val a = JdbcSource("jdbc:h2:mem:test3", "select a,c from mytable where a=4").toDataStream().toVector
      a.head.values.head shouldBe 4
      a.head.values(1) shouldBe 6L
    }
    "read decimal precision and scale" in {
      val conn = DriverManager.getConnection("jdbc:h2:mem:decimal")
      conn.createStatement().executeUpdate("create table mytable (a decimal(15,5))")
      conn.createStatement().executeUpdate("insert into mytable (a) values (1.234)")
      val schema = JdbcSource(() => DriverManager.getConnection("jdbc:h2:mem:decimal"), "select * from mytable").schema
      schema shouldBe
        StructType(Vector(Field("A",DecimalType(Precision(15),Scale(5)))))
    }
    "read numeric precision and scale" in {
      val conn = DriverManager.getConnection("jdbc:h2:mem:numeric")
      conn.createStatement().executeUpdate("create table mytable (a numeric(3,2))")
      conn.createStatement().executeUpdate("insert into mytable (a) values (1.234)")
      val schema = JdbcSource(() => DriverManager.getConnection("jdbc:h2:mem:numeric"), "select * from mytable").schema
      schema shouldBe
        StructType(Vector(Field("A",DecimalType(Precision(3),Scale(2)))))
    }
    "read from jdbc" in {
      val conn = DriverManager.getConnection("jdbc:h2:mem:test4")
      conn.createStatement().executeUpdate("create table mytable (a integer, b bit, c bigint)")
      conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
      conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")
      JdbcSource("jdbc:h2:mem:test4", "select * from mytable").toDataStream().size shouldBe 2
    }
    "support bind" in {
      val conn = DriverManager.getConnection("jdbc:h2:mem:test5")
      conn.createStatement().executeUpdate("create table mytable (a integer, b bit, c bigint)")
      conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('1','2','3')")
      conn.createStatement().executeUpdate("insert into mytable (a,b,c) values ('4','5','6')")
      JdbcSource("jdbc:h2:mem:test5", "select * from mytable where a=?").withBind { it =>
        it.setLong(1, 4)
      }.toDataStream().size shouldBe 1
    }
  }
}
