package com.ran.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
 * 基于服务器log的热门页面浏览量统计
 * 每隔5s，输出最近10分钟内访问量最多的前N个URL
 */

//输入样例类
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

//窗口聚合结果样例类
case class UrlViewCount(url:String,windowEnd:Long,count:Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定时提取更新watermark
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream = env.readTextFile("C:\\Users\\ran\\IdeaProjects\\FlinkProject\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data=>{
        val dataArray = data.split(" ")
        //定义时间转换
        val simpleDataFormat = new SimpleDateFormat("dd/MM/yyy:HH:mm:ss")
        val timestamp = simpleDataFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim,dataArray(1).trim,timestamp,dataArray(5).trim,dataArray(6).trim)
      })     //抽取时间戳的方式
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10),Time.seconds(5))
      .allowedLateness(Time.seconds(60))  //处理迟到数据，没有这个设置迟到的数据会丢掉
      .aggregate(new CountAgg(),new WindowResult())

    val processedStream = dataStream
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dataStream.print("aggregate")
    processedStream.print("process")

    env.execute("network flow job")
  }

}

case class CountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

case class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key,window.getEnd,input.iterator.next()))
  }
}

case class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  //lazy延迟加载，只有在使用该变量时才会加载，类似于Java中单例实现的延迟加载
  lazy val urlState:MapState[String,Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Long]("url-state",classOf[String],classOf[Long]))
  override def processElement(value: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    urlState.put(value.url,value.count)
    //注册定时器
    context.timerService().registerEventTimeTimer(value.windowEnd+1)
  }

  //定时器触发的操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //从状态中拿数据
    val allUrlViews:ListBuffer[(String,Long)] = new ListBuffer[(String, Long)]()
    val iter = urlState.entries().iterator()
    while (iter.hasNext){
      val entry = iter.next()
      allUrlViews += ((entry.getKey,entry.getValue+1))
    }
    val sortedUrlViews = allUrlViews.sortWith(_._2>_._2).take(topSize)

    //格式化输出
    val result:StringBuilder = new StringBuilder()
    //超过窗口右边界触发窗口的处理
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    for(i<-sortedUrlViews.indices){
      val currentUrlView = sortedUrlViews(i)
      result.append("NO").append(i+1).append(":")
        .append(" URL=").append(currentUrlView._1)
        .append(" 访问量=").append(currentUrlView._2).append("\n")
    }
    result.append("==============================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

