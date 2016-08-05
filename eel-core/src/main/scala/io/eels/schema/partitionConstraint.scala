package io.eels.schema

import io.eels.{PartitionPart, PartitionSpec}

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
    partition.parts.find(_.key == name).exists(_.value.compareTo(value) < 0)
}

class PartitionLte(val name: String, val value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
    partition.parts.find(_.key == name).exists(_.value.compareTo(value) <= 0)
}

class PartitionGt(val name: String, val value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
    partition.parts.find(_.key == name).exists(_.value.compareTo(value) > 0)
}

class PartitionGte(val name: String, val value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
    partition.parts.find(_.key == name).exists(_.value.compareTo(value) >= 0)
}