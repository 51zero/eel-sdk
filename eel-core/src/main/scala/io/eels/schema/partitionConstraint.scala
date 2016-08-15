package io.eels.schema

import io.eels.{PartitionPart, PartitionSpec}

trait PartitionConstraint {
  // returns true if the partition name and value matches any of the parts of the given partition
  def eval(partition: PartitionSpec): Boolean
}

case class PartitionEquals(name: String, value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
    partition.parts.contains(PartitionPart(name, value))
}

case class PartitionLt(name: String, value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
    partition.parts.find(_.key == name).exists(_.value.compareTo(value) < 0)
}

case class PartitionLte(name: String, value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
    partition.parts.find(_.key == name).exists(_.value.compareTo(value) <= 0)
}

case class PartitionGt(name: String, value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
    partition.parts.find(_.key == name).exists(_.value.compareTo(value) > 0)
}

case class PartitionGte(name: String, value: String) extends PartitionConstraint {
  override def eval(partition: PartitionSpec): Boolean =
    partition.parts.find(_.key == name).exists(_.value.compareTo(value) >= 0)
}