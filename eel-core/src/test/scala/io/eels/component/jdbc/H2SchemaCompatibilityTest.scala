package io.eels.component.jdbc

import java.sql.DriverManager

import io.eels.schema._
import org.scalatest.{FlatSpec, Matchers}

// tests that the schema mappings for jdbc are correct using h2
class H2SchemaCompatibilityTest extends FlatSpec with Matchers {

  Class.forName("org.h2.Driver")

  val conn = DriverManager.getConnection("jdbc:h2:mem:schematest")
  conn.createStatement().executeUpdate("create table t (" +
    "a integer, " +
    "b bit, " +
    "c bigint, " +
    "d double, " +
    "e time, " +
    "f date, " +
    "g timestamp, " +
    "h decimal(14,5)," +
    "i text," +
    "j smallint," +
    "k tinyint," +
    "l varchar(55)," +
    "m char(15)," +
    "n numeric(66,5)" +
    ")")

  "JdbcSource" should "map schemas from jdbc to correct eel types" in {
    JdbcSource(() => conn, "select * from t").schema shouldBe
      StructType(
        Vector(
          Field("A", IntType(true), true, false),
          Field("B", io.eels.schema.BooleanType, true, false),
          Field("C", io.eels.schema.LongType.Signed, true, false),
          Field("D", io.eels.schema.DoubleType, true, false),
          Field("E", io.eels.schema.TimeMillisType, true, false),
          Field("F", io.eels.schema.DateType, true, false),
          Field("G", io.eels.schema.TimestampMillisType, true, false),
          Field("H", DecimalType(Precision(14), Scale(5)), true, false),
          Field("I", io.eels.schema.StringType, true, false),
          Field("J", io.eels.schema.ShortType(true), true, false),
          Field("K", io.eels.schema.ShortType(true), true, false),
          Field("L", io.eels.schema.VarcharType(55), true, false),
          Field("M", io.eels.schema.CharType(15), true, false),
          Field("N", DecimalType(Precision(66), Scale(5)), true, false)
        )
      )
  }
}
