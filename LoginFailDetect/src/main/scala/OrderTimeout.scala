
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class OrderEvent(
                       orderId: Long, eventType: String, eventTime: Long
                     )

case class OrderResult(
                        orderId: Long, resultMsg: String
                      )

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读入订单数据
    val inputStream: DataStream[OrderEvent] = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "other", 1558430845),
      OrderEvent(2, "pay", 1558430850),
      OrderEvent(1, "pay", 1558431920)
    )).assignAscendingTimestamps(_.eventTime * 1000)


    //定义一个带时间限制的pattern,选出先创建订单，之后又支付的事件流
    val orderTimeoutPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create").followedBy("follow")
      .where(_.eventType == "pay").within(Time.minutes(15))

    //定义一个输出标签标明测输出流
    val outputTag: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")

    //pattern作用到输入流上
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(inputStream.keyBy(_.orderId), orderTimeoutPattern)
    import scala.collection.Map
    //调用select得到最后的符合输出流
    val complexResult: DataStream[OrderResult] = patternStream.select(outputTag)(
      (orderPayEvents: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val timeoutOrderId: Long = orderPayEvents.getOrElse("begin", null).iterator.next().orderId
        OrderResult(timeoutOrderId, "order time out")
      })(
      //pattern select function

      (orderPayEvents: Map[String, Iterable[OrderEvent]]) => {
        val payedOrderId: Long = orderPayEvents.getOrElse("follow", null).iterator.next().orderId
        OrderResult(payedOrderId, "order payed successfully")
      }
    )

    //已正常支付的数据流
    complexResult.print("payed")
    val timeoutResult: DataStream[OrderResult] = complexResult.getSideOutput(outputTag)
    timeoutResult.print("timeout")
    env.execute()
  }
}
