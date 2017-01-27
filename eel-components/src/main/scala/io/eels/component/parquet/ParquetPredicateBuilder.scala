package io.eels.component.parquet

import io.eels._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary

object ParquetPredicateBuilder extends PredicateBuilder[FilterPredicate] {

  override def build(predicate: Predicate): FilterPredicate = {
    predicate match {

      case OrPredicate(predicates) => predicates.map(build).reduceLeft((a, b) => FilterApi.or(a, b))
      case AndPredicate(predicates) => predicates.map(build).reduceLeft((a, b) => FilterApi.and(a, b))

      case EqualsPredicate(name: String, value: String) => FilterApi.eq(FilterApi.binaryColumn(name), Binary.fromConstantByteArray(value.toString().getBytes))
      case EqualsPredicate(name: String, value: Long) => FilterApi.eq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      case EqualsPredicate(name: String, value: Boolean) => FilterApi.eq(FilterApi.booleanColumn(name), java.lang.Boolean.valueOf(value))
      case EqualsPredicate(name: String, value: Float) => FilterApi.eq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case EqualsPredicate(name: String, value: Int) => FilterApi.eq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case EqualsPredicate(name: String, value: Double) => FilterApi.eq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))

      case LtPredicate(name, value: Double) => FilterApi.lt(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case LtPredicate(name, value: Float) => FilterApi.lt(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case LtPredicate(name, value: Int) => FilterApi.lt(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case LtPredicate(name, value: Long) => FilterApi.lt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))

      case LtePredicate(name, value: Double) => FilterApi.ltEq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case LtePredicate(name, value: Float) => FilterApi.ltEq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case LtePredicate(name, value: Int) => FilterApi.ltEq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case LtePredicate(name, value: Long) => FilterApi.ltEq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))

      case GtPredicate(name, value: Double) => FilterApi.gt(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case GtPredicate(name, value: Float) => FilterApi.gt(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case GtPredicate(name, value: Int) => FilterApi.gt(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case GtPredicate(name, value: Long) => FilterApi.gt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))

      case GtePredicate(name, value: Double) => FilterApi.gtEq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case GtePredicate(name, value: Float) => FilterApi.gtEq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case GtePredicate(name, value: Int) => FilterApi.gtEq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case GtePredicate(name, value: Long) => FilterApi.gtEq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
    }
  }
}
