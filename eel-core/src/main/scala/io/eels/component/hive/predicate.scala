package io.eels.component.hive

import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary

trait Predicate {
  def apply(): FilterPredicate
}

object Predicate {
  def or(left: Predicate, right: Predicate): Predicate = new Predicate {
    override def apply(): FilterPredicate = FilterApi.or(left(), right())
  }
}

object PredicateEquals {
  def apply(name: String, value: String): Predicate = new Predicate {
    override def apply(): FilterPredicate = {
      FilterApi.eq(FilterApi.binaryColumn(name), Binary.fromConstantByteArray(value.toString.getBytes))
    }
  }
  def apply(name: String, value: Long): Predicate = new Predicate {
    override def apply(): FilterPredicate = {
      FilterApi.eq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
  }
}