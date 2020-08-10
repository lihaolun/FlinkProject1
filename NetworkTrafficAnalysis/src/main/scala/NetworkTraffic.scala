import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//weblog样例类
//83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
case class ApacheLogEvent(ip: String, userName: String, eventTime: Long, method: String, url: String)

//中间统计数量的数据类型
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkTraffic {
  def main(args: Array[String]): Unit = {

    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //数据源
    val inputPath = "F:\\WorkSpace\\inteliJ\\UserBehaviorAnalysis\\NetworkTrafficAnalysis\\src\\main\\resources\\apache.log"
    //转换
    val dataStream = env.readTextFile(inputPath)
      .map(data => {
        val dataArray = data.split(" ")
        // 把log时间转换为时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
        ApacheLogEvent(dataArray(0), dataArray(2), timestamp, dataArray(5), dataArray(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          // 已经是毫秒
          element.eventTime
        }
      })
      .keyBy(_.url) // 根据url分组
      .timeWindow(Time.minutes(1), Time.seconds(5)) // 开滑动窗口
      .aggregate(new CountAgg(), new WindowResultFuntion())
      .keyBy(_.windowEnd) // 根据时间窗口来分组
      .process(new TopNurls(5))
      .print() // sink 输出
    env.execute("Network Traffic Analysis")
  }
  }

  class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFuntion() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url = key
      val windowEnd: Long = window.getEnd
      val count: Long = input.iterator.next()
      out.collect(UrlViewCount(url, windowEnd, count))
    }

  }

  class TopNurls(topN: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url_state", classOf[UrlViewCount]))

    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {

      urlState.add(i)

      context.timerService().registerEventTimeTimer(i.windowEnd + 100)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allUrlViewCounts: ListBuffer[UrlViewCount] = ListBuffer()

      /* for(i <- urlState.get()){
         allUrlViewCounts += i
       }*/
      val iter = urlState.get().iterator()
      while (iter.hasNext)
        allUrlViewCounts += iter.next()
      urlState.clear()

      val urlViewCountBuffer: ListBuffer[UrlViewCount] = allUrlViewCounts.sortWith(_.count > _.count).take(topN)

      val stringBuilder = new StringBuilder()
      stringBuilder.append("时间:").append(new Timestamp(timestamp - 100)).append("\n")
      for (i <- urlViewCountBuffer.indices) {
        val currendUrlView = urlViewCountBuffer(i)
        stringBuilder.append("No").append(i + 1).append(":").append(" url=").append(currendUrlView.url).append(" 流量=").append(currendUrlView.count).append("\n")
      }
      Thread.sleep(1000)
      out.collect(stringBuilder.toString())

    }

}
