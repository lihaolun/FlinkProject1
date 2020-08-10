import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class LoginEvent(
                       userId: Long, ip: String, eventType: String, eventTime: Long
                     )

case class Warning(
                    userid: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String
                  )

object LoginFail {
  def main(args: Array[String]): Unit = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //转换
    val loginFailStream: DataStreamSink[Warning] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignAscendingTimestamps(_.eventTime * 1000).keyBy(_.userId).process(new MatchFunction()).print("loginFail")
    env.execute("login Fail Detect")

  }
}

class MatchFunction() extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state", classOf[LoginEvent]))

  /* override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
     if (i.eventType == "fail") {
       loginState.add(i)
       context.timerService().registerEventTimeTimer((i.eventTime + 2) * 1000)
     }
   }

   override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
     val allLogins:ListBuffer[LoginEvent] = ListBuffer()

     val iter: util.Iterator[LoginEvent] = loginState.get().iterator()
     while(iter.hasNext){
       allLogins+=iter.next()
     }
     loginState.clear()

     //如果state长度大于1，说明有两个以上的登陆失败时间，输出报警信息
     if(allLogins.length>1){
       out.collect(Warning(allLogins.head.userId,
         allLogins.head.eventTime,
         allLogins.last.eventTime,
         "login fail in 2 seconds for" + allLogins.length + " items."
       ))
     }

   }*/

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    //首先按照type做筛选，如果success直接清空，如果fail再做处理
    if (i.eventType == "fail") {
      // 如果已经有登录失败的数据，那么就判断是否在两秒内
      val iter: util.Iterator[LoginEvent] = loginState.get().iterator()
      if (iter.hasNext()) {
        val firstFail: LoginEvent = iter.next()
        if (i.eventTime - 2 < firstFail.eventTime) {
          collector.collect(Warning(i.userId, firstFail.eventTime, i.eventTime, "login fail"))
        }
        val failList = new util.ArrayList[LoginEvent]()
        failList.add(i)
        loginState.update(failList)
      } else {
        //如果state中没有登录失败的数据，那就直接添加进去
        loginState.add(i)
      }

    } else {
      loginState.clear()
    }


  }
}