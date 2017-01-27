package io.eels.component.orc

import io.eels._
import org.apache.hadoop.hive.ql.io.sarg.{PredicateLeaf, SearchArgument, SearchArgumentFactory}

object OrcPredicateBuilder extends PredicateBuilder[SearchArgument] {

  override def build(predicate: Predicate): SearchArgument = {
    val builder = SearchArgumentFactory.newBuilder().startOr()
    build(predicate, builder)
    builder.end.build()
  }

  private def not(builder: SearchArgument.Builder, around: => Any): Unit = {
    builder.startNot()
    around
    builder.end()
  }

  private def build(predicate: Predicate, builder: SearchArgument.Builder): Unit = predicate match {

    case OrPredicate(predicates) =>
      builder.startOr()
      predicates.foreach(build(_, builder))
      builder.end()

    case AndPredicate(predicates) =>
      builder.startAnd()
      predicates.foreach(build(_, builder))
      builder.end()

    case EqualsPredicate(name: String, value: String) => builder.equals(name, PredicateLeaf.Type.STRING, value)
    case EqualsPredicate(name: String, value: Boolean) => builder.equals(name, PredicateLeaf.Type.BOOLEAN, value)
    case EqualsPredicate(name: String, value: Double) => builder.equals(name, PredicateLeaf.Type.FLOAT, java.lang.Double.valueOf(value))
    case EqualsPredicate(name: String, value: Float) => builder.equals(name, PredicateLeaf.Type.FLOAT, java.lang.Float.valueOf(value))
    case EqualsPredicate(name: String, value: Int) => builder.equals(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value))
    case EqualsPredicate(name: String, value: Long) => builder.equals(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value))

    case LtPredicate(name, value: Double) => builder.lessThan(name, PredicateLeaf.Type.FLOAT, java.lang.Double.valueOf(value))
    case LtPredicate(name, value: Float) => builder.lessThan(name, PredicateLeaf.Type.FLOAT, java.lang.Float.valueOf(value))
    case LtPredicate(name, value: Int) => builder.lessThan(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value))
    case LtPredicate(name, value: Long) => builder.lessThan(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value))

    case LtePredicate(name, value: Double) => builder.lessThanEquals(name, PredicateLeaf.Type.FLOAT, java.lang.Double.valueOf(value))
    case LtePredicate(name, value: Float) => builder.lessThanEquals(name, PredicateLeaf.Type.FLOAT, java.lang.Float.valueOf(value))
    case LtePredicate(name, value: Int) => builder.lessThanEquals(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value))
    case LtePredicate(name, value: Long) => builder.lessThanEquals(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value))

    case GtPredicate(name, value: Double) => not(builder, builder.lessThanEquals(name, PredicateLeaf.Type.FLOAT, java.lang.Double.valueOf(value)))
    case GtPredicate(name, value: Float) => not(builder, builder.lessThanEquals(name, PredicateLeaf.Type.FLOAT, java.lang.Float.valueOf(value)))
    case GtPredicate(name, value: Int) => not(builder, builder.lessThanEquals(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value)))
    case GtPredicate(name, value: Long) => not(builder, builder.lessThanEquals(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value)))

    case GtePredicate(name, value: Double) => not(builder, builder.lessThan(name, PredicateLeaf.Type.FLOAT, java.lang.Double.valueOf(value)))
    case GtePredicate(name, value: Float) => not(builder, builder.lessThan(name, PredicateLeaf.Type.FLOAT, java.lang.Float.valueOf(value)))
    case GtePredicate(name, value: Int) => not(builder, builder.lessThan(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value)))
    case GtePredicate(name, value: Long) => not(builder, builder.lessThan(name, PredicateLeaf.Type.LONG, java.lang.Long.valueOf(value)))
  }
}
