package com.ran.networkflow

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * 网站总浏览量PV的统计
 * 实时统计每个小时内的网站PV
 */

//定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data=>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)   //数据源是有序的
      .filter(_.behavior=="pv")
      .map(x=>("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count")

    env.execute("page view job")
  }
}
