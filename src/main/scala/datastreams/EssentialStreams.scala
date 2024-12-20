package datastreams

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object EssentialStreams {

  private def applicationTemplate(): Unit = {
    // execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // in between, add any sort of computations
    import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data of your DataStreams
    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4)

    // perform some actions
    simpleNumberStream.print()

    // at the end
    env.execute() // trigger all the computations that were DESCRIBED earlier
  }

  //transformations
  private def transformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //check parallelism
    println(s"Current parallelism ${env.getParallelism}") // number of cored
    //set
    env.setParallelism(2)
    println(s"New parallelism ${env.getParallelism}")
    val stream        = env.fromElements(1, 2, 3, 4)
    val doubleNumbers = stream.map(_ * 2)

    //flatmap
    val expandedNumbers = stream.flatMap(x => List(x, x + 1))

    //filter
    val filteredNumbers = stream.filter(_ % 2 == 0)

    expandedNumbers.writeAsText("output/expandedNumbers.txt").setParallelism(1)

    env.execute()
  }

  /**
   * exercise:
   * - stream of natural numbers
   * - for every number
   * if n % 3 == 0 => "fizz"
   * if n % 5 == 0 => "buzz"
   * if n % 3 == 0 && n % 5 == 0 => "fizzbuzz"
   *  -write numbers for which you said fizzbuzz to a file
   */

  def ex() = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val naturalNumbers = env.fromElements((0 to 100): _*)
    val fizz           = naturalNumbers.filter(_ % 3 == 0).map(_ => "fizz")
    val buzz           = naturalNumbers.filter(_ % 5 == 0).map(_ => "buzz")
    val fizzbuzz = naturalNumbers
      .filter(_ % 3 == 0)
      .filter(_ % 5 == 0)
      .addSink(
        StreamingFileSink
          .forRowFormat(
            new Path("output/streaming_sink"),
            new SimpleStringEncoder[Int]("UTF-8")
          )
          .build()
      )
      .setParallelism(1)

    env.execute()
  }

  def explicitTransformations(): Unit = {
    val env     = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers = env.fromSequence(0, 100)

    // map
    val doubleNumbers = numbers.map(_ * 2)
    // explicit map
    val doubleNumbersExplicit = numbers.map(new MapFunction[Long, Long] {
      override def map(value: Long): Long = value * 2
    })

    val expandedNumbers = numbers.flatMap(n => (1L to n).toList)
    val expandedNumbersExplicit = numbers.flatMap(new FlatMapFunction[Long, Long] {
      override def flatMap(value: Long, out: Collector[Long]): Unit =
        (1L to value).foreach(i => out.collect(i))
    })

    //process method
    val expandedNumbersProcess = numbers.process(new ProcessFunction[Long, Long] {
      override def processElement(value: Long, ctx: ProcessFunction[Long, Long]#Context, out: Collector[Long]): Unit =
        (1L to value).foreach(i => out.collect(i))
    })
    //reduce
    //happens on keyed streams
    val keyedNumbers: KeyedStream[Long, Boolean] = numbers.keyBy(n => n % 2 == 0)
    val sumByKey                                 = keyedNumbers.reduce(_ + _) // sum up all elements by key
    val sumByKeyV2 = keyedNumbers.reduce(new ReduceFunction[Long] {
      override def reduce(value1: Long, value2: Long): Long = value1 + value2
    })
  }
  def main(args: Array[String]): Unit =
    ex()
}
