package io.eels.component.parquet

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary

trait Predicate {
  def apply(): FilterPredicate
}

object Predicate {

  def or(left: Predicate, right: Predicate): Predicate = new Predicate {
    override def apply(): FilterPredicate = FilterApi.or(left.apply(), right.apply())
  }

  def and(left: Predicate, right: Predicate): Predicate = new Predicate {
    override def apply(): FilterPredicate = FilterApi.and(left.apply(), right.apply())
  }

  def equals(name: String, value: String): Predicate = new Predicate {
    override def apply(): FilterPredicate = {
      FilterApi.eq(FilterApi.binaryColumn(name), Binary.fromConstantByteArray(value.toString().getBytes))
    }
  }

  def equals(name: String, value: Long): Predicate = new Predicate {
    override def apply(): FilterPredicate = {
      FilterApi.eq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
  }

  def equals(name: String, value: Boolean): Predicate = new Predicate {
    override def apply(): FilterPredicate = {
      FilterApi.eq(FilterApi.booleanColumn(name), java.lang.Boolean.valueOf(value))
    }
  }

  def gt(name: String, value: Long): Predicate = new Predicate {
    override def apply(): FilterPredicate = {
      FilterApi.gt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
  }

  def lt(name: String, value: Long): Predicate = new Predicate {
    override def apply(): FilterPredicate = {
      FilterApi.lt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
  }
}