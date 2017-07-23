package io.eels.schema

import org.scalatest.{FunSuite, Matchers}

class SchemaFromClassTest extends FunSuite with Matchers {

  case class Foo1(a: String, b: Long, c: Double)
  case class Foo2(a: BigDecimal)
  case class Foo3(foo: Foo2)

  test("schema from class") {
    StructType.from[Foo1] shouldBe
      StructType(Vector(Field("a", StringType), Field("b", LongType.Signed), Field("c", DoubleType)))
  }

  test("schema from class should support big decimals") {
    StructType.from[Foo2] shouldBe StructType(Vector(Field("a", DecimalType(Precision(22), Scale(5)))))
  }

  test("schema from class should support nested case classes") {
    StructType.from[Foo3] shouldBe StructType(
      Vector(
        Field("foo", StructType(
          Vector(
            Field("a", DecimalType(Precision(22), Scale(5)))
          )
        ), false)
      )
    )
  }
}
