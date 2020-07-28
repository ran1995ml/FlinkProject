package com.ran.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 网站独立访问数UV的统计
 */

case class UvCount(windowEnd:Long,uvCount:Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data=>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior=="pv")
      .timeWindowAll(Time.hours(1))   //不需要按key聚合
        .apply(new UvCountByWindow())     //apply，处理窗口数据，传入参数windowfunction或者是allwindowfunction

    dataStream.print()
    env.execute("uv job")
  }

}

case class UvCountByWindow() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个set，保存所有数据的userId并去重
    var idSet = Set[Long]()
    //把当前窗口所有数据的ID收集到set中，最后输出set的大小
    for(userBehavior<-input){
      idSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd,idSet.size))
  }
}

