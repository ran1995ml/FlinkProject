package com.ran.networkflow

/**
 * 所有数据的userId都存在窗口计算的状态里，在窗口收集数据的过程中，状态不断增大。可以放在redis中缓存，但是数据量非常大呢？
 * 我们不需要完整存储用户ID的信息，可以对ID信息压缩处理，用一位（bit）表示用户的状态，具体实现就是布隆过滤器。
 * 本身是一个很长的二进制向量，更高效，占用空间更少，缺点是返回的结果是概率性的不是确切的。
 * 利用某种方法，将数据对应到位图的某一位上去，数据存在就是1，否则为0.
 *
 */

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvCountWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //用相对路径定义数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data=>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toInt,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)
      .filter(_.behavior=="pv")
      .map(data=>("dummyKey",data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())          //自定义窗口触发规则
      .process(new UvCountWithBloom())   //自定义窗口处理规则

    dataStream.print()
    env.execute("uv with bloom job")
  }

}

//自定义窗口触发器
case class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    //每来一条数据，就直接触发窗口操作，清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}

//定义一个布隆过滤器
class Bloom(size:Long) extends Serializable{
  //位图总大小，默认16M
  private val cap = if (size > 0) size else 1 << 27

  //定义hash函数
  def hash(value:String,seed:Int):Long={
    var result = 0L
    for(i<-0 until value.length){
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }
}


case class UvCountWithBloom() extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{
  //定义redis连接
  lazy val jedis = new Jedis("node01",6379)
  lazy val bloom = new Bloom(1<<28)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //位图存储方式，key是windowEnd，value是bitmap
    val storeKey = context.window.getEnd.toString
    var count = 0L
    //把每个窗口的uv count值也存入名为count的redis表，存放内容（windowEnd->uvCount)，要先从redis读取
    if(jedis.hget("count",storeKey)!=null){
      count = jedis.hget("count",storeKey).toLong
    }
    //用布隆过滤器判断用户是否存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId,61)
    //定义一个标识位，判断redis位图中有没有这一位
    val isExist = jedis.getbit(storeKey,offset)
    if(!isExist){
      //如果不存在，位图对应位置1
      jedis.setbit(storeKey,offset,true)
      jedis.hset("count",storeKey,(count+1).toString)
      out.collect(UvCount(storeKey.toLong,count+1))
    }else{
      out.collect(UvCount(storeKey.toLong,count))
    }
  }
}
