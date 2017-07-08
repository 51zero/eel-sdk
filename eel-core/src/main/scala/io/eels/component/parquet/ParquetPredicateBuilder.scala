package io.eels.component.parquet

import io.eels.{AndPredicate, EqualsPredicate, GroupStats, GtPredicate, GtePredicate, LtPredicate, LtePredicate, NotEqualsPredicate, NotPredicate, OrPredicate, Predicate, PredicateBuilder}
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate, UserDefinedPredicate, Statistics => Stats}
import org.apache.parquet.io.api.Binary

// builds parquet predicates from eel predicates
object ParquetPredicateBuilder extends PredicateBuilder[FilterPredicate] {

  def udp[T <: Comparable[T]](u: io.eels.UserDefinedPredicate[T]): UserDefinedPredicate[T] with Serializable = new UserDefinedPredicate[T] with Serializable {
    override def canDrop(stats: Stats[T]): Boolean = u.canDropGroup(GroupStats[T](stats.getMin, stats.getMax))
    override def inverseCanDrop(stats: Stats[T]): Boolean = u.canDropGroup(GroupStats[T](stats.getMin, stats.getMax))
    override def keep(value: T): Boolean = u.keep(value)
  }

  override def build(predicate: Predicate): FilterPredicate = {
    predicate match {

      case OrPredicate(predicates) => predicates.map(build).reduceLeft((a, b) => FilterApi.or(a, b))
      case AndPredicate(predicates) => predicates.map(build).reduceLeft((a, b) => FilterApi.and(a, b))

      case NotPredicate(inner) => FilterApi.not(build(inner))

      case u: io.eels.UserDefinedPredicate[_] => FilterApi.userDefined(FilterApi.binaryColumn(u.name), udp[Binary](u.asInstanceOf[io.eels.UserDefinedPredicate[Binary]]))

      case NotEqualsPredicate(name: String, value: String) => FilterApi.notEq(FilterApi.binaryColumn(name), Binary.fromConstantByteArray(value.toString().getBytes))
      case NotEqualsPredicate(name: String, value: Long) => FilterApi.notEq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      case NotEqualsPredicate(name: String, value: Boolean) => FilterApi.notEq(FilterApi.booleanColumn(name), java.lang.Boolean.valueOf(value))
      case NotEqualsPredicate(name: String, value: Float) => FilterApi.notEq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case NotEqualsPredicate(name: String, value: Int) => FilterApi.notEq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case NotEqualsPredicate(name: String, value: Double) => FilterApi.notEq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case NotEqualsPredicate(name: String, big: BigInt) =>
        FilterApi.userDefined(FilterApi.binaryColumn(name), new UserDefinedPredicate[Binary] with Serializable {
          override def canDrop(statistics: Stats[Binary]): Boolean = false
          override def inverseCanDrop(statistics: Stats[Binary]): Boolean = false
          override def keep(value: Binary): Boolean = {
            BigInt(value.getBytes) != big
          }
        })

      case EqualsPredicate(name: String, value: String) => FilterApi.eq(FilterApi.binaryColumn(name), Binary.fromConstantByteArray(value.toString().getBytes))
      case EqualsPredicate(name: String, value: Long) => FilterApi.eq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      case EqualsPredicate(name: String, value: Boolean) => FilterApi.eq(FilterApi.booleanColumn(name), java.lang.Boolean.valueOf(value))
      case EqualsPredicate(name: String, value: Float) => FilterApi.eq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case EqualsPredicate(name: String, value: Int) => FilterApi.eq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case EqualsPredicate(name: String, value: Double) => FilterApi.eq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case EqualsPredicate(name: String, big: BigInt) =>
        FilterApi.userDefined(FilterApi.binaryColumn(name), new UserDefinedPredicate[Binary] with Serializable {
          override def canDrop(statistics: Stats[Binary]): Boolean = {
            BigInt(statistics.getMin.getBytes) > big || BigInt(statistics.getMax.getBytes) < big
          }
          override def inverseCanDrop(statistics: Stats[Binary]): Boolean = false
          override def keep(value: Binary): Boolean = BigInt(value.getBytes) == big
        })

      case LtPredicate(name, value: Double) => FilterApi.lt(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case LtPredicate(name, value: Float) => FilterApi.lt(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case LtPredicate(name, value: Int) => FilterApi.lt(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case LtPredicate(name, value: Long) => FilterApi.lt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      case LtPredicate(name: String, big: BigInt) =>
        FilterApi.userDefined(FilterApi.binaryColumn(name), new UserDefinedPredicate[Binary] with Serializable {
          override def canDrop(statistics: Stats[Binary]): Boolean = {
            BigInt(statistics.getMin.getBytes) >= big
          }
          override def inverseCanDrop(statistics: Stats[Binary]): Boolean = {
            BigInt(statistics.getMax.getBytes) < big
          }
          override def keep(value: Binary): Boolean = BigInt(value.getBytes).compare(big) < 0
        })

      case LtePredicate(name, value: Double) => FilterApi.ltEq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case LtePredicate(name, value: Float) => FilterApi.ltEq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case LtePredicate(name, value: Int) => FilterApi.ltEq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case LtePredicate(name, value: Long) => FilterApi.ltEq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      case LtePredicate(name: String, big: BigInt) =>
        FilterApi.userDefined(FilterApi.binaryColumn(name), new UserDefinedPredicate[Binary] with Serializable {
          override def canDrop(statistics: Stats[Binary]): Boolean = {
            BigInt(statistics.getMin.getBytes) > big
          }
          override def inverseCanDrop(statistics: Stats[Binary]): Boolean = {
            BigInt(statistics.getMax.getBytes) <= big
          }
          override def keep(value: Binary): Boolean = BigInt(value.getBytes).compare(big) <= 0
        })

      case GtPredicate(name, value: Double) => FilterApi.gt(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case GtPredicate(name, value: Float) => FilterApi.gt(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case GtPredicate(name, value: Int) => FilterApi.gt(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case GtPredicate(name, value: Long) => FilterApi.gt(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      case GtPredicate(name, big: BigInt) =>
        FilterApi.userDefined(FilterApi.binaryColumn(name), new UserDefinedPredicate[Binary] with Serializable {
          override def canDrop(statistics: Stats[Binary]): Boolean = {
            BigInt(statistics.getMax.getBytes) <= big
          }
          override def inverseCanDrop(statistics: Stats[Binary]): Boolean = {
            BigInt(statistics.getMin.getBytes) > big
          }
          override def keep(value: Binary): Boolean = BigInt(value.getBytes).compare(big) > 0
        })

      case GtePredicate(name, value: Double) => FilterApi.gtEq(FilterApi.doubleColumn(name), java.lang.Double.valueOf(value))
      case GtePredicate(name, value: Float) => FilterApi.gtEq(FilterApi.floatColumn(name), java.lang.Float.valueOf(value))
      case GtePredicate(name, value: Int) => FilterApi.gtEq(FilterApi.intColumn(name), java.lang.Integer.valueOf(value))
      case GtePredicate(name, value: Long) => FilterApi.gtEq(FilterApi.longColumn(name), java.lang.Long.valueOf(value))
      case GtePredicate(name, big: BigInt) =>
        FilterApi.userDefined(FilterApi.binaryColumn(name), new UserDefinedPredicate[Binary] with Serializable {
          override def canDrop(statistics: Stats[Binary]): Boolean = {
            BigInt(statistics.getMax.getBytes) < big
          }
          override def inverseCanDrop(statistics: Stats[Binary]): Boolean = {
            BigInt(statistics.getMin.getBytes) >= big
          }
          override def keep(value: Binary): Boolean = BigInt(value.getBytes).compare(big) >= 0
        })

      case _ => sys.error("Unsupported predicate")
    }
  }
}
