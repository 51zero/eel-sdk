package io.eels.component.jdbc

import java.sql.DriverManager

import io.eels.schema._
import org.scalatest.{FlatSpec, Matchers}

// tests that the schema mappings for jdbc are correct using h2
class H2SchemaCompatibilityTest extends FlatSpec with Matchers {

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
          Field("A", IntType(true), true, false, None, Map()),
          Field("B", io.eels.schema.BooleanType, true, false, None, Map()),
          Field("C", io.eels.schema.BigIntType, true, false, None, Map()),
          Field("D", io.eels.schema.DoubleType, true, false, None, Map()),
          Field("E", io.eels.schema.TimeMillisType, true, false, None, Map()),
          Field("F", io.eels.schema.DateType, true, false, None, Map()),
          Field("G", io.eels.schema.TimestampMillisType, true, false, None, Map()),
          Field("H", DecimalType(Precision(14), Scale(5)), true, false, None, Map()),
          Field("I", io.eels.schema.StringType, true, false, None, Map()),
          Field("J", io.eels.schema.ShortType(true), true, false, None, Map()),
          Field("K", io.eels.schema.ShortType(true), true, false, None, Map()),
          Field("L", io.eels.schema.VarcharType(55), true, false, None, Map()),
          Field("M", io.eels.schema.CharType(15), true, false, None, Map()),
          Field("N", DecimalType(Precision(66), Scale(5)), true, false, None, Map())
        )
      )
  }
}
