package io.eels.schema

import io.eels.component.hive.PartitionPart
import io.eels.component.hive.PartitionSpec
import io.eels.util.findOptional

trait PartitionConstraint {
  // returns true if the partition name and value matches any of the parts of the given partition
  def eval(partition: PartitionSpec): Boolean
}

class PartitionEquals(val name: String, val value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
      partition.parts.contains(PartitionPart(name, value))
}

class PartitionLt(val name: String, val value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
      partition.parts.findOptional { it.key == name }.exists { it.value.compareTo(value) < 0 }
}

class PartitionLte(val name: String, val value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
      partition.parts.findOptional { it.key == name }.exists { it.value.compareTo(value) <= 0 }
}

class PartitionGt(val name: String, val value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
      partition.parts.findOptional { it.key == name }.exists { it.value.compareTo(value) > 0 }
}

class PartitionGte(val name: String, val value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
      partition.parts.findOptional { it.key == name }.exists { it.value.compareTo(value) >= 0 }
}