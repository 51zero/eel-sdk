package io.eels.component.parquet

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary

trait Predicate {
  def parquet(): FilterPredicate
}

object Predicate {

  def or(left: Predicate, right: Predicate): Predicate = new Predicate {
    override def parquet(): FilterPredicate = FilterApi.or(left.parquet(), right.parquet())
  }

  def and(left: Predicate, right: Predicate): Predicate = new Predicate {
    override def parquet(): FilterPredicate = FilterApi.and(left.parquet(), right.parquet())
  }

  def equals(name: String, value: String): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.eq(FilterApi.binaryColumn(name), Binary.fromConstantByteArray(value.toString().getBytes))
    }
  }

  def equals(name: String, value: Long): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.eq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
  }

  def equals(name: String, value: Boolean): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.eq(FilterApi.booleanColumn(name), java.lang.Boolean.valueOf(value))
    }
  }

  def gt(name: String, value: Long): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.gt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
  }

  def lt(name: String, value: Long): Predicate = new Predicate {
    override def parquet(): FilterPredicate = {
      FilterApi.lt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
  }
}