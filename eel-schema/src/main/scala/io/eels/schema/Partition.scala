package io.eels.schema

// Represents a partition, which is the full sequence of partition key/values pairs for a particular record
// eg key1=value1,key2=value2,key3=value3 is a partition
case class Partition(entries: Seq[PartitionEntry]) {

  def name: String = unquoted
  def unquoted: String = entries.map(_.unquoted).mkString("/")

  def keys: Seq[String] = entries.map(_.key)

  def values: Seq[String] = entries.map(_.value)

  // returns the PartitionEntry for the given key
  def get(key: String): Option[PartitionEntry] = entries.find(_.key == key)

  def value(key: String): Option[String] = get(key).map(_.value)

  def containsKey(key: String): Boolean = entries.exists(_.key == key)
}

object Partition {
  val empty = Partition(Nil)
  def apply(first: PartitionEntry, rest: PartitionEntry*): Partition = Partition(first +: rest)
  def apply(first: (String, Any), rest: (String, Any)*): Partition = apply((first +: rest).map { case (key, value) => PartitionEntry(key, value.toString) })
}

// a part of a partition, ie in country=usa/state=alabama, an entry would be state=alabama or country=usa
case class PartitionEntry(key: String, value: String) {

  // returns the key value part in the standard hive key=value format with unquoted values
  def unquoted(): String = s"$key=$value"

  // returns the key value part in the standard hive key=value format with quoted values
  def quoted(): String = s"$key='$value'"
}