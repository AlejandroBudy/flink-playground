package datastreams

import org.apache.flink.streaming.api.scala._
import generators.shopping.ShoppingCartEventsGenerator
import org.apache.flink.streaming.api.windowing.time.Time
import datastreams.TimeBasedTransformations.CountByWindow
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger

object Triggers {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def demoFirstTrigger: Unit = {
    val shoppingEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(sleepMillisPerEvent = 500, batchSize = 2)) // 2 events / second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))                         // 10 evetns/window
      .trigger(CountTrigger.of[TimeWindow](5))                                              // trigger window function (CountByWindow) after 5 elements, not when the widow ends
      .process(new CountByWindow())                                                         // runs twice for the same window

    shoppingEvents.print()
    env.execute()

    /*
     Cada ventana dura 5 segundos y se procesan 10 eventos por ventana. El trigger se dispara cuando se procesan 5 eventos y cuenta cuantos eventos hay en la ventana
     por lo tanto se ejecuta dos veces por ventana, la primera vez cuenta 5 eventos y la segunda vez cuenta 10 eventos.
        [info] 8> Window [1741370565000-1741370570000]: 5 events
        [info] 9> Window [1741370570000-1741370575000]: 5 events
        [info] 10> Window [1741370570000-1741370575000]: 10 events
        [info] 11> Window [1741370575000-1741370580000]: 5 events
        [info] 1> Window [1741370575000-1741370580000]: 10 events
        [info] 2> Window [1741370580000-1741370585000]: 5 events
        [info] 3> Window [1741370580000-1741370585000]: 10 events
        [info] 4> Window [1741370585000-1741370590000]: 5 events
        [info] 5> Window [1741370585000-1741370590000]: 10 events
        [info] 6> Window [1741370590000-1741370595000]: 5 event
     */
  }

  // purging trigger - clear the window when it fires

  def demoPurgingTrigger = {

    val shoppingEvents: DataStream[String] = env
      .addSource(new ShoppingCartEventsGenerator(sleepMillisPerEvent = 500, batchSize = 2)) // 2 events / second
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))                         // 10 evetns/window
      .trigger(PurgingTrigger.of(CountTrigger.of[TimeWindow](5)))                           // trigger window function (CountByWindow) after 5 elements, not when the widow ends
      .process(new CountByWindow())                                                         // runs twice for the same window

    shoppingEvents.print()
    env.execute()

    /*
     * En este caso el trigger limpia la ventana cuando la window function termina, por lo tanto la primera vez que se ejecuta se cuentan 5 eventos y se limpia la ventana
     * entran los siguientes 5 eventos en la ventana y se vuelven a ejecturar la window function y se limpia la ventana.
    [info] 6> Window [1741371035000-1741371040000]: 5 events
    [info] 7> Window [1741371040000-1741371045000]: 5 events
    [info] 8> Window [1741371040000-1741371045000]: 5 events
    [info] 9> Window [1741371045000-1741371050000]: 5 events
    [info] 10> Window [1741371045000-1741371050000]: 5 events
     */
  }

  def main(args: Array[String]): Unit =
    demoPurgingTrigger
}
