package datastreams

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import generators.shopping._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.co.CoProcessFunction

object MultipleStreams {

  // unioning = combining the output of multiple streams into just one
  def demoUnion() = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEventsKafka: DataStream[ShoppingCartEvent] = env.addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Some("kafka")))
    val shoppingCartEventsFiles: DataStream[ShoppingCartEvent] = env.addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Some("files")))

    val combinedShoppingCartEvents = shoppingCartEventsKafka.union(shoppingCartEventsFiles)

    combinedShoppingCartEvents.print()
    env.execute()
  }

  // window join = elements belong to the same window + some join condition
  def demoJoin() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents: DataStream[ShoppingCartEvent] = env.addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Some("kafka")))
    val catalogEvents                                     = env.addSource(new CatalogEventsGenerator(1000))

    val joinedStream =
      shoppingCartEvents
        .join(catalogEvents)
        // provide a join condition
        .where(shoppingCartEvent => shoppingCartEvent.userId)
        .equalTo(catalogEvent => catalogEvent.userId)
        // provide the same window
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        // do something with the joined elements
        .apply((cartEvent, catalogEvent) => s"User id ${cartEvent.userId} browsed at ${catalogEvent.time} and bought at ${cartEvent.time}")

    joinedStream.print()
    env.execute()

  }

  //interval joins = correlation between events A and B if durationMin < timeA - timeB < durationMax
  //this involves EVENT TIME
  // only works in keyed streams
  def demoInterval() = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // we need to extract event times from both streams
    val shoppingCartEvents = env
      .addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Some("kafka")))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // max delay < 500ms
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = element.time.toEpochMilli
          })
      )
      .keyBy(_.userId)

    val catalogEvents = env
      .addSource(new CatalogEventsGenerator(1000))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // max delay < 500ms
          .withTimestampAssigner(new SerializableTimestampAssigner[CatalogEvent] {
            def extractTimestamp(element: CatalogEvent, recordTimestamp: Long): Long = element.time.toEpochMilli
          })
      )
      .keyBy(_.userId)

    val intervalJoinedStreams =
      shoppingCartEvents
        .intervalJoin(catalogEvents)
        .between(Time.seconds(-2), Time.seconds(2))
        .lowerBoundExclusive()
        .upperBoundExclusive()
        .process(new ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String] {
          override def processElement(
              left: ShoppingCartEvent,
              right: CatalogEvent,
              ctx: ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String]#Context,
              out: Collector[String]
          ): Unit =
            out.collect(s"User ${left.userId} bought at ${left.time} and browsed at ${right.time}")
        })

    intervalJoinedStreams.print()
    env.execute()

  }

  //connect = two streams are treated with the same operator

  def demoConnect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(100, sourceId = Some("kafka"))).setParallelism(1)
    val catalogEvents      = env.addSource(new CatalogEventsGenerator(1000)).setParallelism(1)

    // connect the two streams
    val connectedStreams: ConnectedStreams[ShoppingCartEvent, CatalogEvent] = shoppingCartEvents.connect(catalogEvents)

    //variables  - will use single-threaded
    env.setParallelism(1)
    env.setMaxParallelism(1)

    val ratioSteam: DataStream[Double] = connectedStreams.process(
      new CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double] {

        var shoppingCartEventsCount = 0
        var catalogEventsCount      = 0

        override def processElement1(
            value: ShoppingCartEvent,
            ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
            out: Collector[Double]
        ): Unit = {
          shoppingCartEventsCount += 1
          out.collect(shoppingCartEventsCount * 100.0 / (shoppingCartEventsCount + catalogEventsCount))
        }

        override def processElement2(
            value: CatalogEvent,
            ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
            out: Collector[Double]
        ): Unit = {
          catalogEventsCount += 1
          out.collect(shoppingCartEventsCount * 100.0 / (shoppingCartEventsCount + catalogEventsCount))
        }
      }
    )

    ratioSteam.print()
    env.execute()
  }
  def main(args: Array[String]): Unit =
    demoConnect()
}
