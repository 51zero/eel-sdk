package io.eels.schema

// Represents a hive partition, which, against what you might think, in hive speak
// is the full set of partition key/values pairs for a particular record
// eg key1=value1/key2=value2/key3=value3 is a partition
// The individual partition key value pairs don't seem to have a name in Hive so I have decided
// to call them PartitionPart
data class Partition(val parts: List<PartitionPart>) {

  // returns the partition in normalized directory representation, eg key1=value1/key2=value2/...
  // hive seems to call this the partition name, at least client.listPartitionNames returns these
  fun name(): String = parts.map { it.unquoted() }.joinToString ("/")

  // from key1=value1/key2=value2 will return key1,key2
  fun keys(): List<String> = parts.map { it.key }

  // from key1=value1/key2=value2 will return List(value1,value2)
  fun values(): List<String> = parts.map { it.value }

  // returns the partition value for the given key
  fun get(key: String): String? = parts.find { it.key == key }?.value

  companion object {
    fun parsePath(path: String): Partition {
      val parts = path.split("/").map {
        val (key, value) = it.split("=")
        PartitionPart(key, value)
      }
      return Partition(parts)
    }

    val empty = Partition(emptyList())
  }
}

// represents part of a partition, eg a single key=value pair, which is what people might
// think a partition is normally. I've decided to call it PartitionPart
data class PartitionPart(val key: String, val value: String) {
  // returns the key value part in the standard hive key=value format
  fun unquoted(): String = "$key=$value"
}

// represents the name of a single partition key
data class PartitionKey(val name: String)

// represents a single partition key and all its values
data class PartitionPartValues(val key: PartitionKey, val values: List<String>)