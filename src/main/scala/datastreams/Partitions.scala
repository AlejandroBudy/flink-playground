package datastreams

import org.apache.flink.streaming.api.scala._
import generators.shopping._
import org.apache.flink.api.common.functions.Partitioner

object Partitions {

  // splitting = partitioning
  def demoPartitioning(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEnvents = env.addSource(new SingleShoppingCartEventsGenerator(100)) // 10 events/s

    // partitioner is the logic to split the data
    val partitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = {
        // hash code % number of partitions ˜ even distribution
        println(s"Number of partitions: $numPartitions")
        key.hashCode % numPartitions
      }
    }

    val partitionedStream = shoppingCartEnvents.partitionCustom(partitioner, event => event.userId)

    partitionedStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit =
    demoPartitioning()
}
