package io.eels.component.hive

import io.eels.schema.Field
import org.apache.hadoop.hive.metastore.api.StorageDescriptor

// Represents a partition, which is the full set of partition key/values pairs for a particular record
// eg key1=value1/key2=value2/key3=value3 is a partition

@Deprecated("use partition actual")
data class PartitionSpec(val parts: List<PartitionPart>) {

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
    fun parsePath(path: String): PartitionSpec {
      val parts = path.split("/").map {
        val (key, value) = it.split("=")
        PartitionPart(key, value)
      }
      return PartitionSpec(parts)
    }

    val empty = PartitionSpec(emptyList())
  }
}

@Deprecated("use partition actual")
data class PartitionPart(val key: String, val value: String) {
  // returns the key value part in the standard hive key=value format
  fun unquoted(): String = "$key=$value"
}

data class PartitionKey(val field: Field,
                        val createTime: Long,
                        val parameters: Map<String, String>)

// Represents a partition, which is the full set of partition key/values pairs for a particular record
// eg key1=value1/key2=value2/key3=value3 is a partition
data class Partition(val createTime: Long,
                     val sd: StorageDescriptor,
                     val values: List<PartitionKeyValue>) {
  // returns the partition in normalized directory representation, eg key1=value1/key2=value2/...
  // hive calls this the partition name, eg client.listPartitionNames returns these
  fun name(): String = values.map { it.unquoted() }.joinToString ("/")
}

// a single "part" in a partition, ie in country=usa/state=alabama, a value would be state=alabama
// todo better name for this, like PartitionPart or PartitionPath or something
data class PartitionKeyValue(val key: PartitionKey, val value: String) {

  // returns the key value part in the standard hive key=value format with unquoted values
  fun unquoted(): String = "${key.field.name}=$value"

  // returns the key value part in the standard hive key=value format with quoted values
  fun quoted(): String = "${key.field.name}='$value'"
}