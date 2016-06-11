package io.eels.schema

import io.eels.util.findOptional

interface PartitionConstraint {
  // returns true if the partition name and value matches any of the parts of the given partition
  fun eval(partition: Partition): Boolean
}

class PartitionEquals(val name: String, val value: String) : PartitionConstraint {
  override fun eval(partition: Partition): Boolean =
      partition.parts.contains(PartitionPart(name, value))
}

class PartitionLt(val name: String, val value: String) : PartitionConstraint {
  override fun eval(partition: Partition): Boolean =
      partition.parts.findOptional { it.key == name }.exists { it.value.compareTo(value) < 0 }
}

class PartitionLte(val name: String, val value: String) : PartitionConstraint {
  override fun eval(partition: Partition): Boolean =
      partition.parts.findOptional { it.key == name }.exists { it.value.compareTo(value) <= 0 }
}

class PartitionGt(val name: String, val value: String) : PartitionConstraint {
  override fun eval(partition: Partition): Boolean =
      partition.parts.findOptional { it.key == name }.exists { it.value.compareTo(value) > 0 }
}

class PartitionGte(val name: String, val value: String) : PartitionConstraint {
  override fun eval(partition: Partition): Boolean =
      partition.parts.findOptional { it.key == name }.exists { it.value.compareTo(value) >= 0 }
}