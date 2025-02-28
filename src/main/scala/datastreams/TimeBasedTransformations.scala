package datastreams

import org.apache.flink.streaming.api.scala._
import generators.shopping._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.api.common.eventtime.WatermarkGenerator
import org.apache.flink.api.common.eventtime.WatermarkOutput
import org.apache.flink.api.common.eventtime.Watermark

object TimeBasedTransformations {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingEvents = env.addSource(
    new ShoppingCartEventsGenerator(
      sleepMillisPerEvent = 100,
      batchSize = 5,
      baseInstant = java.time.Instant.parse("2022-02-15T00:00:00.00Z")
    )
  )

  class CountByWindow extends ProcessAllWindowFunction[ShoppingCartEvent, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val window = context.window
      out.collect(s"Window [${window.getStart}-${window.getEnd}]: ${elements.size} events")
    }
  }
  /*
   * with processing time = when the event was processed by Flink
   * - we don't care when the event was created
   * - multiple run produce different results
   * */
// group by window, every 3s, tumbling (non-overlapping), PROCESSING TIME
  def demoProcessinTime(): Unit = {
    def groupEventsByWindow                     = shoppingEvents.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
    def countEventsByWindow: DataStream[String] = groupEventsByWindow.process(new CountByWindow)
    countEventsByWindow.print()
    env.execute()
  }

  /*
   * with event time = when the event was created
   * - we need to care about handling late data - done with Watermarks
   * - we dont care about flink's internal time
   * - we might see faster results
   * - multiple runs produce the same results
   * */
  def demoEventTime(): Unit = {
    val eventsByWindow = shoppingEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // max delay < 500ms
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    def countEventsByWindow: DataStream[String] = eventsByWindow.process(new CountByWindow)
    countEventsByWindow.print()
    env.execute()
  }

  //Custom watermark
  // with every new MAX timestamp, every event with timestamp < MAX - maxDelay will be considered late
  class BoundedOutOfOrderness(maxDelay: Long) extends WatermarkGenerator[ShoppingCartEvent] {
    var currentMaxTimestamp = 0L
    // maybe emit a watermark on every event - not needed to emit a watermark
    override def onEvent(event: ShoppingCartEvent, eventTimestamp: Long, output: WatermarkOutput): Unit =
      //                   ^ event being processed    ^ timestamp attached to the event
      currentMaxTimestamp = Math.max(currentMaxTimestamp, event.time.toEpochMilli)
    // emit a watermark is not mandatory
    // output.emitWatermark(...)

    // Flink can also call this method regularly - up to us to emit a watermark
    override def onPeriodicEmit(output: WatermarkOutput): Unit =
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxDelay - 1))
  }
  def demoEventTime_v2(): Unit = {
    // control how often Flink calls onPeriodicEmit
    env.getConfig.setAutoWatermarkInterval(1000L)

    val eventsByWindow = shoppingEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forGenerator(_ => new BoundedOutOfOrderness(500))
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = element.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    def countEventsByWindow: DataStream[String] = eventsByWindow.process(new CountByWindow)
    countEventsByWindow.print()
    env.execute()
  }

  def main(args: Array[String]): Unit =
    /*
      [info] 10> Window [1740771993000-1740771996000]: 10 events
      [info] 11> Window [1740771996000-1740771999000]: 25 events
      [info] 1> Window [1740771999000-1740772002000]: 30 events
      [info] 2> Window [1740772002000-1740772005000]: 30 events
      [info] 3> Window [1740772005000-1740772008000]: 30 events
      [info] 4> Window [1740772008000-1740772011000]: 30 events
     */
    //demoProcessinTime()

    /*
      [info] 10> Window [1644883200000-1644883203000]: 5 events
      [info] 11> Window [1644883203000-1644883206000]: 5 events
      [info] 1> Window [1644883209000-1644883212000]: 5 events
      [info] 2> Window [1644883215000-1644883218000]: 5 events
      [info] 3> Window [1644883218000-1644883221000]: 5 events
      [info] 4> Window [1644883224000-1644883227000]: 5 events
      [info] 5> Window [1644883230000-1644883233000]: 5 events
      [info] 6> Window [1644883233000-1644883236000]: 5 events
      [info] 7> Window [1644883239000-1644883242000]: 5 events
      [info] 8> Window [1644883245000-1644883248000]: 5 events
      [info] 9> Window [1644883248000-1644883251000]: 5 events
      [info] 10> Window [1644883254000-1644883257000]: 5 events
     */
    //demoEventTime()

    /*
      [info] 10> Window [1644883200000-1644883203000]: 5 events
      [info] 11> Window [1644883203000-1644883206000]: 5 events
      [info] 1> Window [1644883209000-1644883212000]: 5 events
      [info] 2> Window [1644883215000-1644883218000]: 5 events
      [info] 3> Window [1644883218000-1644883221000]: 5 events
      [info] 4> Window [1644883224000-1644883227000]: 5 events
      [info] 5> Window [1644883230000-1644883233000]: 5 events
      [info] 6> Window [1644883233000-1644883236000]: 5 events
      [info] 7> Window [1644883239000-1644883242000]: 5 events
      [info] 8> Window [1644883245000-1644883248000]: 5 events
      [info] 9> Window [1644883248000-1644883251000]: 5 events
      [info] 10> Window [1644883254000-1644883257000]: 5 events
     */
    demoEventTime_v2() // Los eventos son los mismos
}
