
import java.util

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //数据源
    val inputStream: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "success", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(1, "192.168.0.3", "fail", 1558430847),
      LoginEvent(1, "192.168.0.3", "fail", 1558430848),
      LoginEvent(2, "192.168.10.10", "success", 1558430850)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)

    //定义pattern，对事件流进行匹配
    val loginPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail").next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))


    //在输入流的基础上应用pattern，得到匹配的pattern stream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(inputStream, loginPattern)

    //用select方法从pattern stream中提取输出流
    import scala.collection.Map
    val loginFailDS: DataStreamSink[Warning] = patternStream.select((patternEvents: Map[String, Iterable[LoginEvent]]) => {
      val firstFailEvent = patternEvents.getOrElse("begin", null).iterator.next()
      val lastFailEvent = patternEvents.getOrElse("next", null).iterator.next()
      Warning(firstFailEvent.userId, firstFailEvent.eventTime, lastFailEvent.eventTime, "login fail warning")
    }).print("Login Fail WithCep")

   // patternStream.select(new LoginPatternFunction()).print("Login Fail WithCep")
    env.execute("LoginFailWithCep")
  }


/*class LoginPatternFunction extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val firstFailEvent = map.getOrDefault("begin", null).iterator().next()
    val lastFailEvent = map.getOrDefault("next", null).iterator().next()
    Warning(firstFailEvent.userId, firstFailEvent.eventTime, lastFailEvent.eventTime, "login fail warning")

  }*/

}
