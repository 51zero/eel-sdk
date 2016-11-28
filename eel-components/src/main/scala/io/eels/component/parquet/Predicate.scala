package io.eels.component.parquet

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary

trait Predicate {
  def parquet(): FilterPredicate
  // returns a list of fields that this predicate operates on
  def fields(): List[String]
}

object Predicate {

  def or(left: Predicate, right: Predicate): Predicate = new Predicate {
    override def parquet(): FilterPredicate = FilterApi.or(left.parquet(), right.parquet())
    override def fields(): List[String] = left.fields() ++ right.fields()
  }

  def and(left: Predicate, right: Predicate): Predicate = new Predicate {
    override def parquet(): FilterPredicate = FilterApi.and(left.parquet(), right.parquet())
    override def fields(): List[String] = left.fields() ++ right.fields()
  }

  def equals(name: String, value: String): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.eq(FilterApi.binaryColumn(name), Binary.fromConstantByteArray(value.toString().getBytes))
    }
    override def fields(): List[String] = List(name)
  }

  def equals(name: String, value: Long): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.eq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def equals(name: String, value: Boolean): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.eq(FilterApi.booleanColumn(name), java.lang.Boolean.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def equals(name: String, value: Float): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.eq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def equals(name: String, value: Int): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.eq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def equals(name: String, value: Double): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.eq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def gt(name: String, value: Long): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.gt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def gt(name: String, value: Int): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.gt(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def gt(name: String, value: Double): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.gt(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def gt(name: String, value: Float): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.gt(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def lt(name: String, value: Double): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.lt(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def lt(name: String, value: Float): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.lt(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def lt(name: String, value: Int): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.lt(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def lt(name: String, value: Long): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.lt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def gte(name: String, value: Long): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.gtEq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def gte(name: String, value: Int): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.gtEq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def gte(name: String, value: Double): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.gtEq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def gte(name: String, value: Float): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.gtEq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def lte(name: String, value: Double): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.ltEq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def lte(name: String, value: Long): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.ltEq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def lte(name: String, value: Int): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.ltEq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }

  def lte(name: String, value: Float): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.ltEq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
    }
    override def fields(): List[String] = List(name)
  }
}