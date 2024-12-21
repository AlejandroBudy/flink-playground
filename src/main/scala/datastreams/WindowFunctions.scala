package datastreams

import generators.gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Instant
import scala.concurrent.duration._

object WindowFunctions {

  //use case for gaming events
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  implicit val serverStartTime = Instant.parse("2022-02-02T00:00:00.000Z")
  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player bob registered 2 seconds after the server started
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.second),
    alice.register(4.seconds),
    mary.register(5.seconds),
    mary.online(6.seconds),
    carl.online(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  //how many players were registered every 3 seconds
  val eventStream: DataStream[ServerEvent] =
    env
      .fromCollection(events)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) //once you get an event don't accept events with T - 500ms
          .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
            override def extractTimestamp(element: ServerEvent, recordTimestamp: Long): Long = element.eventTime.toEpochMilli
          })
      )

  val threeSecondsWindow = eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(_.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart}-${window.getEnd}]: $registrationEventCount")
    }
  }

  def demoCountByWindow(): Unit = {
    val registrationCounter = threeSecondsWindow.apply(new CountByWindowAll)
    registrationCounter.print()
    env.execute()
  }

  class CountByWindowAll_2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {

    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window                 = context.window
      val registrationEventCount = elements.count(_.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart}-${window.getEnd}]: $registrationEventCount")
    }
  }

  def demoProcessCount(): Unit = {
    val registrationCounter = threeSecondsWindow.process(new CountByWindowAll_2)
    registrationCounter.print()
    env.execute()
  }

  //alternative 2: aggregate
  class CountByWindowAll_3 extends AggregateFunction[ServerEvent, Long, Long] {

    override def add(value: ServerEvent, accumulator: Long): Long =
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator

    override def createAccumulator(): Long          = 0L
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(a: Long, b: Long): Long      = a + b
  }

  def demoAggregate() = {
    val counter = threeSecondsWindow.aggregate(new CountByWindowAll_3)
    counter.print()
    env.execute()
  }

  /*
  keyed streams and window functions
   */
// elements will be assigned to a mini-stream for its own key
  val streamByType: KeyedStream[ServerEvent, String] = eventStream.keyBy(e => e.getClass.getSimpleName)

  // for every key we'll have a separate window
  val threeSecondsTumblingWindow = streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  class CountInWindow extends WindowFunction[ServerEvent, String, String, TimeWindow] {

    override def apply(key: String, window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit =
      out.collect(s"$key: $window, ${input.size}")
  }

  def demoCountByTypeByWindow (): Unit = {
   val finalStream = threeSecondsTumblingWindow.apply(new CountInWindow)
    finalStream.print()
    env.execute()

  }

  def main(args: Array[String]): Unit =
    demoCountByTypeByWindow()
}
