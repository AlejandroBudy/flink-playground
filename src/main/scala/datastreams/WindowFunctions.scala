package datastreams

import generators.gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.time.Instant
import scala.concurrent.duration._

object WindowFunctions {

  //use case for gaming events
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  implicit val serverStartTime = Instant.parse("2022-02-02T00:00:00.000Z")
  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player "Bob" registered 2s after the server started
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
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

  /*
      |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
      |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
      |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
      |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |
      ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
      |                                     |                                             |                                           |                                    |
      |            1 registrations          |               3 registrations               |              2 registration               |            0 registrations         |
      |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
   */

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

  def demoCountByTypeByWindow(): Unit = {
    val finalStream = threeSecondsTumblingWindow.apply(new CountInWindow)
    finalStream.print()
    env.execute()

  }

  class CountInWindow_2 extends ProcessWindowFunction[ServerEvent, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      out.collect(s"$key: $window, ${elements.size}")
    }
  }

  def demoProcessCountByTypeByWindow(): Unit = {
    val finalStream = threeSecondsTumblingWindow.process(new CountInWindow_2)
    finalStream.print()
    env.execute()
  }

  /*
  sliding windows
   */

  // how many players were registered every 3 seconds, UPDATED EVERY 1s?
  // [0s..3s] [1s..4s] [2s..5s] [3s..6s] ...

  /*
  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    | carl online  |
  |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
  ^|------------ window one ----------- +
                1 registration

            + ---------------- window two --------------- +
                                  2 registrations

                       + ------------------- window three ------------------- +
                                           4 registrations

                                         + ---------------- window four --------------- +
                                                          3 registrations

                                                          + ---------------- window five -------------- +
                                                                           3 registrations

                                                                               + ---------- window six -------- +
                                                                                           1 registration

                                                                                        + ------------ window seven ----------- +
                                                                                                    2 registrations

                                                                                                         + ------- window eight------- +
                                                                                                                  1 registration

                                                                                                                + ----------- window nine ----------- +
                                                                                                                        1 registration

                                                                                                                                   + ---------- window ten --------- +
                                                                                                                                              0 registrations
   */
  def demoSlidingWindow() = {
    val windowSize = Time.seconds(3)
    val slideBy    = Time.seconds(1)

    val slidingWindowsAll = eventStream.windowAll(SlidingEventTimeWindows.of(windowSize, slideBy))

    val registrationCountByWindow = slidingWindowsAll.apply(new CountByWindowAll)
    registrationCountByWindow.print()
    env.execute()
  }

  /*
   * session windows = groups of events with no more than a certain time gap between them
   */

  // how many events do we have no more than 1s apart?

  /*
     |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
     |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
     |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
     |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |

     after filtering:

     +---------+---------+-----------------+-----------------+-------------------+-------+-----------------+-------+-----------------+-----+---------------+--------------+
     |         |         | bob registered  | sam registered  | rob registered    |       | mary registered |       | carl registered |     |     N/A       |              |
     |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
                         ^ ----------------- window 1 -------------------------- ^       ^ -- window 2 --- ^       ^ -- window 3 --- ^     ^ -- window 4 - ^
   */

  def demoSessionWindow() = {
    val groupBySessionsWindows = eventStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))
    val countBySessionWindows  = groupBySessionsWindows.apply(new CountByWindowAll)
    countBySessionWindows.print()
    env.execute()
  }

  /*
  Global windows
   */

  //how many registration events do we have in every 10 events
  class CountByGlobalWindowAll extends AllWindowFunction[ServerEvent, String, GlobalWindow] {
    override def apply(window: GlobalWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(_.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [$window]: $registrationEventCount")
    }
  }

  def demoGlobalWindow() = {
    eventStream
      .windowAll(GlobalWindows.create())
      .trigger(CountTrigger.of[GlobalWindow](10))
      .apply(new CountByGlobalWindowAll)
      .print()

    env.execute()
  }

  def main(args: Array[String]): Unit =
    demoGlobalWindow()
}
