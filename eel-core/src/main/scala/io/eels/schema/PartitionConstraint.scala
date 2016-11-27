package io.eels.schema

trait PartitionConstraint {
  // returns true if the partition name and value matches any of the parts of the given partition
  def eval(partition: PartitionSpec): Boolean
}

object PartitionConstraint {

  def or(left: PartitionConstraint, right: PartitionConstraint): PartitionConstraint = new PartitionConstraint {
    def eval(partition: PartitionSpec): Boolean = {
      left.eval(partition) || right.eval(partition)
    }
  }

  def and(left: PartitionConstraint, right: PartitionConstraint): PartitionConstraint = new PartitionConstraint {
    def eval(partition: PartitionSpec): Boolean = {
      left.eval(partition) && right.eval(partition)
    }
  }

  def equals(name: String, value: Any): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: PartitionSpec): Boolean =
      partition.parts.contains(PartitionPart(name, value.toString))
  }

  def lt(name: String, value: String): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: PartitionSpec): Boolean =
      partition.parts.find(_.key == name).exists(_.value.compareTo(value) < 0)
  }

  def lte(name: String, value: String): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: PartitionSpec): Boolean =
      partition.parts.find(_.key == name).exists(_.value.compareTo(value) <= 0)
  }

  def gt(name: String, value: String): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: PartitionSpec): Boolean =
      partition.parts.find(_.key == name).exists(_.value.compareTo(value) > 0)
  }

  def gte(name: String, value: String): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: PartitionSpec): Boolean =
      partition.parts.find(_.key == name).exists(_.value.compareTo(value) >= 0)
  }
}

