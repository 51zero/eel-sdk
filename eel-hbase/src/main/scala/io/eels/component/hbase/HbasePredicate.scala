package io.eels.component.hbase

import io.eels._
import io.eels.schema.{DataType, StructType}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

// These are simply marker predicates used for pattern matching
case class ContainsPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = true
}

case class RegexPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = true
}

case class StartsWithPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = true
}

case class NotEqualsPredicate(name: String, value: Any) extends NamedPredicate(name) {
  override def eval(row: Row): Boolean = row.get(name) != value
}

object HbasePredicate {

  private val ByteComparableClazz = classOf[BinaryComparator]
  private val StringComparableClazz = classOf[SubstringComparator]
  private val RegexStringComparableClazz = classOf[RegexStringComparator]
  private val BinaryPrefixComparableClazz = classOf[BinaryPrefixComparator]

  def apply(pred: Predicate)(implicit schema: StructType, serializer: HbaseSerializer): FilterList = pred match {
    case EqualsPredicate(name, value) => new FilterList(hbaseFiler(name, CompareOp.EQUAL, value, ByteComparableClazz))
    case NotEqualsPredicate(name, value) => new FilterList(hbaseFiler(name, CompareOp.NOT_EQUAL, value, ByteComparableClazz))
    case ContainsPredicate(name, value) => new FilterList(hbaseFiler(name, CompareOp.EQUAL, value, StringComparableClazz))
    case StartsWithPredicate(name, value) => new FilterList(hbaseFiler(name, CompareOp.EQUAL, value, BinaryPrefixComparableClazz))
    case RegexPredicate(name, value) => new FilterList(hbaseFiler(name, CompareOp.EQUAL, value, RegexStringComparableClazz))
    case GtPredicate(name, value) => new FilterList(hbaseFiler(name, CompareOp.GREATER, value, ByteComparableClazz))
    case GtePredicate(name, value) => new FilterList(hbaseFiler(name, CompareOp.GREATER_OR_EQUAL, value, ByteComparableClazz))
    case LtPredicate(name, value) => new FilterList(hbaseFiler(name, CompareOp.LESS, value, ByteComparableClazz))
    case LtePredicate(name, value) => new FilterList(hbaseFiler(name, CompareOp.LESS_OR_EQUAL, value, ByteComparableClazz))
    case AndPredicate(predicates: Seq[Predicate]) => new FilterList(FilterList.Operator.MUST_PASS_ALL, predicates.map(apply).flatMap(_.getFilters))
    case OrPredicate(predicates: Seq[Predicate]) => new FilterList(FilterList.Operator.MUST_PASS_ONE, predicates.map(apply).flatMap(_.getFilters))
    case _@predicateType => sys.error(s"Predicate type '${predicateType.getClass.getSimpleName}' is not supported!")
  }

  def hbaseFiler[T](name: String, compareOp: CompareOp, value: Any, comparableClass: Class[T])
                   (implicit schema: StructType, serializer: HbaseSerializer): Filter = {
    val field = schema.fields.find(_.name == name).getOrElse(sys.error(s"Field '$name' in the predicate is not defined in the EEL schema"))
    if (field.key) {
      new RowFilter(compareOp, hbaseComparator(comparableClass, name, field.dataType, value))
    } else {
      new SingleColumnValueFilter(
        Bytes.toBytes(field.columnFamily.getOrElse(sys.error(s"No Column Family defined for field '${field.name}'"))),
        Bytes.toBytes(name),
        compareOp,
        hbaseComparator(comparableClass, name, field.dataType, value))
    }
  }

  def hbaseComparator[T](comparableClass: Class[T], name: String, dataType: DataType, value: Any)
                        (implicit schema: StructType, serializer: HbaseSerializer): ByteArrayComparable = (comparableClass, value) match {
    case (ByteComparableClazz, _) => new BinaryComparator(serializer.toBytes(value, name, dataType))
    case (RegexStringComparableClazz, stringValue: String) => new RegexStringComparator(stringValue)
    case (StringComparableClazz, stringValue: String) => new SubstringComparator(stringValue)
    case (BinaryPrefixComparableClazz, _) => new BinaryPrefixComparator(serializer.toBytes(value, name, dataType))
  }

  // Shorthand predicate names
  def or(left: Predicate, right: Predicate) = OrPredicate(Seq(left, right))

  def and(left: Predicate, right: Predicate) = AndPredicate(Seq(left, right))

  def equals(name: String, value: Any) = EqualsPredicate(name, value)

  def notEquals(name: String, value: Any) = NotEqualsPredicate(name, value)

  def gt(name: String, value: Any) = GtPredicate(name, value)

  def gte(name: String, value: Any) = GtePredicate(name, value)

  def lt(name: String, value: Any) = LtPredicate(name, value)

  def lte(name: String, value: Any) = LtePredicate(name, value)

  def regex(name: String, value: Any) = RegexPredicate(name, value)

  def contains(name: String, value: Any) = ContainsPredicate(name, value)

  def startsWith(name: String, value: Any) = StartsWithPredicate(name, value)

}
