package io.eels.component

import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.io.api.Binary

interface Predicate {

  fun apply(): FilterPredicate

  companion object {

    fun or(left: Predicate, right: Predicate): Predicate = object : Predicate {
      override fun apply(): FilterPredicate = FilterApi.or(left.apply(), right.apply())
    }

    fun and(left: Predicate, right: Predicate): Predicate = object : Predicate {
      override fun apply(): FilterPredicate = FilterApi.and(left.apply(), right.apply())
    }

    fun equals(name: String, value: String): Predicate = object : Predicate {
      override fun apply(): FilterPredicate {
        return FilterApi.eq(FilterApi.binaryColumn(name), Binary.fromConstantByteArray(value.toString().toByteArray()))
      }
    }

    fun equals(name: String, value: Long): Predicate = object : Predicate {
      override fun apply(): FilterPredicate {
        return FilterApi.eq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      }
    }

    fun equals(name: String, value: Boolean): Predicate = object : Predicate {
      override fun apply(): FilterPredicate {
        return FilterApi.eq(FilterApi.booleanColumn(name), java.lang.Boolean.valueOf(value))
      }
    }

    fun gt(name: String, value: Long): Predicate = object : Predicate {
      override fun apply(): FilterPredicate {
        return FilterApi.gt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      }
    }

    fun lt(name: String, value: Long): Predicate = object : Predicate {
      override fun apply(): FilterPredicate {
        return FilterApi.lt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      }
    }

  }
}