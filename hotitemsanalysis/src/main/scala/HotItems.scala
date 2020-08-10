import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class UserBehavior(
                         userid: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long
                       )

case class ItemViewCount(
                          itemId: Long, windowEnd: Long, count: Long
                        )

object HotItems {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    //指定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据进行处理
    val inputPath = "F:\\WorkSpace\\inteliJ\\UserBehaviorAnalysis\\hotitemsanalysis\\src\\main\\resources\\UserBehavior.csv";
    /*    val transStream: DataStream[UserBehavior] = env.readTextFile(inputPath).map(fun = data => {
          val dataArray: Array[String] = data.split(",")
          UserBehavior(dataArray(0).trim().toLong, dataArray(1).trim().toLong, dataArray(2).trim().toInt, dataArray(3), dataArray(4).trim().toLong)
        }
        )*/

    val transStream: DataStream[String] = env.readTextFile(inputPath).map(line => {
      val lineArray: Array[String] = line.split(",")
      UserBehavior(lineArray(0).trim.toLong, lineArray(1).trim.toLong, lineArray(2).trim.toInt, lineArray(3).trim, lineArray(4).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000).filter(_.behavior == "pv").keyBy(_.itemId)
      .timeWindow(Time.minutes(60), Time.minutes(5)) //开时间窗口，滑动窗口
      .aggregate(new CountAgg(), new WindowResultFuntion()).keyBy(_.windowEnd).process(new TopNHotItems(3))

    transStream.print("transStream")

    env.execute("HotItems")
  }
}

//预聚合操作，来一条数据就计数器加1
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResultFuntion() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}


class TopNHotItems(size: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 获取当前运行环境中的 ListState，用来回复itemState
    val itemStateDesc = new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount])
    itemState = getRuntimeContext.getListState(itemStateDesc)
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //每条数据暂存入liststate
    itemState.add(i)
    //注册一个定时器，延迟触发
    context.timerService().registerEventTimeTimer(i.windowEnd + 100)
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()

    import scala.collection.JavaConversions._
    for (i <- itemState.get()) {
      allItems += i
    }
    //提前清除状态数据
    itemState.clear()

    //按照count大小排序选择前size个
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(size)

    //将最终结果转换为String输出

    val stringBuilder = new StringBuilder
    stringBuilder.append("时间：").append(timestamp - 100).append("\n")
    for (i <- 0 until sortedItems.length) {
      val currentItem = sortedItems(i)
      stringBuilder.append(i + 1).append(":")
        .append("商品ID=").append(currentItem.itemId).append("浏览量=").append(currentItem.count).append("\n")

    }
    Thread.sleep(1000)
    out.collect(stringBuilder.toString())
  }
}