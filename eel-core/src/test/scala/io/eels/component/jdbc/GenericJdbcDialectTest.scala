package io.eels.component.jdbc

import java.sql.Types
import javax.sql.rowset.RowSetMetaDataImpl

import io.eels.component.jdbc.dialect.GenericJdbcDialect
import io.eels.schema._
import org.scalatest.{Matchers, WordSpec}

class GenericJdbcDialectTest extends WordSpec with Matchers {

  "GenericJdbcDialect.fromJdbcType" should {
    "convert int field to signed IntType" in {
      val meta = new RowSetMetaDataImpl
      meta.setColumnCount(1)
      meta.setColumnType(1, Types.INTEGER)
      new GenericJdbcDialect().fromJdbcType(1, meta) shouldBe IntType.Signed
    }
    "convert bigint field to signed LongType" in {
      val meta = new RowSetMetaDataImpl
      meta.setColumnCount(1)
      meta.setColumnType(1, Types.BIGINT)
      new GenericJdbcDialect().fromJdbcType(1, meta) shouldBe LongType.Signed
    }
  }

  "GenericJdbcDialect" should {
    "convert int field to int type" in {
      new GenericJdbcDialect().toJdbcType(Field("a", IntType(true), false)) shouldBe "int"
    }
    "convert Boolean field to int type" in {
      new GenericJdbcDialect().toJdbcType(Field("b", BooleanType, false)) shouldBe "boolean"
    }
    "convert short field to SMALLINT type" in {
      new GenericJdbcDialect().toJdbcType(Field("a", ShortType.Signed, false)) shouldBe "smallint"
    }
    "convert String field to text" in {
      new GenericJdbcDialect().toJdbcType(Field("a", StringType, false)) shouldBe "text"
    }
    "convert varchar field to VARCHAR using size" in {
      new GenericJdbcDialect().toJdbcType(Field("a", VarcharType(242), false)) shouldBe "varchar(242)"
    }
  }
}
