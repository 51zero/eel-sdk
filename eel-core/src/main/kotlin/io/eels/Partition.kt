package io.eels

// represents a hive partition, which, against what you might think, in hive speak is the full list of
// partition key values pairs for a record, eg key1=value1/key2=value2/key3=value3 is the partition, key=value is not a partition,
data class Partition(val parts: List<PartitionPart>) {

  // returns the partition in normalized directory representation, eg key1=value1/key2=value2/...
  // hive seems to call this the partition name, at least client.listPartitionNames returns these
  fun name(): String = parts.map { it.unquoted() }.joinToString ("/")

  // from key1=value1/key2=value2 will return key1,key2
  fun keys(): List<String> = parts.map { it.key }

  // from key1=value1/key2=value2 will return value1,value2
  fun values(): List<String> = parts.map { it.value }

  // returns the partition value for the given key
  fun get(key: String): String? = parts.find { it.key == key }.map { it.value }
}

// represents part of a partition, eg a single key=value pair, which is what people might think a
// partition is normally. For example, name=sam is not a partition in hive speak (it doesn't seem to have a name in hive)
// so I've decided to call it PartitionPart
data class PartitionPart(val key: String, val value: String) {
  // returns the key value part in the standard hive key=value format
  fun unquoted(): String = "$key=$value"
}

// represents a single partition key and all its values
// todo need a better name
data class PartitionKey(val key: String, val values: List<String>)