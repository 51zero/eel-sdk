package io.eels.schema

trait PartitionConstraint {
  // returns true if the partition name and value matches any of the parts of the given partition
  def eval(partition: Partition): Boolean
}

object PartitionConstraint {

  def or(left: PartitionConstraint, right: PartitionConstraint): PartitionConstraint = new PartitionConstraint {
    def eval(partition: Partition): Boolean = left.eval(partition) || right.eval(partition)
  }

  def and(left: PartitionConstraint, right: PartitionConstraint): PartitionConstraint = new PartitionConstraint {
    def eval(partition: Partition): Boolean = left.eval(partition) && right.eval(partition)
  }

  def equals(key: String, value: Any): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: Partition): Boolean =
      partition.entries.contains(PartitionEntry(key, value.toString))
  }

  def lt(key: String, value: String): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: Partition): Boolean =
      partition.entries.find(_.key == key).exists(_.value.compareTo(value) < 0)
  }

  def lte(key: String, value: String): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: Partition): Boolean =
      partition.entries.find(_.key == key).exists(_.value.compareTo(value) <= 0)
  }

  def gt(key: String, value: String): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: Partition): Boolean =
      partition.entries.find(_.key == key).exists(_.value.compareTo(value) > 0)
  }

  def gte(key: String, value: String): PartitionConstraint = new PartitionConstraint {
    override def eval(partition: Partition): Boolean =
      partition.entries.find(_.key == key).exists(_.value.compareTo(value) >= 0)
  }
}

