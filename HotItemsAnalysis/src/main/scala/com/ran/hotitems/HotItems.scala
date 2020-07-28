package com.ran.hotitems

/**
 * 需求：每隔5分钟输出最近一小时内点击量最多的前N个商品
 */


import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
//定义窗口聚合结果的样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    //1.创建执行环境，程序执行的context
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","node01:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")

    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
      .map(data=>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)  //数据没有乱序，用此法，只需要指定时间戳

    //3.transform 处理数据
    val processedStream = dataStream
      .filter(_.behavior=="pv")   //过滤出点击行为
      .keyBy(_.itemId)    //对商品进行分组
      .timeWindow(Time.hours(1),Time.minutes(5))    //对每个商品设置滑动窗口
      .aggregate(new CountAgg(),new WindowResult())      //提前聚合掉数据，减少state的存储压力，不用缓存整个窗口的数据
      .keyBy(_.windowEnd)    //按窗口末尾分组
      .process(new TopHotItems(3))

    //4.sink 控制台输出
    processedStream.print()
    env.execute("hot items job")
  }


}

//自定义预聚合函数
case class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  //初始值
  override def createAccumulator(): Long = 0L
  //每个分区计算数量
  override def add(in: UserBehavior, acc: Long): Long = acc + 1
  //输出结果
  override def getResult(acc: Long): Long = acc
  //多个分区的结果合并
  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

//自定义窗口输出函数
case class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}

case class TopHotItems(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  //存储状态，避免故障时状态丢失，ListState集成了checkpoint机制，自动做到exactly-once语义
  private var itemState:ListState[ItemViewCount] = _

  //rich function的初始化方法，算子被调用之前先调用open
  override def open(parameters: Configuration): Unit = {
    //函数执行时的context，可获得函数运行时的并行度，任务名字和状态，命名状态遍历的名字和类型
    //定义状态变量
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))
  }


  override def processElement(input: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //将每条数据保存到状态中
    itemState.add(input)
    //注册windowend+1的定时器，一旦触发，说明收齐了属于windowend窗口的所有商品数据
    context.timerService().registerEventTimeTimer(input.windowEnd+1)
  }

  //定时器触发时，对所有数据排序，输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //将所有state的数据取出，放到一个List Buffer中
    val allitems:ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for(item<-itemState.get()){
      allitems += item
    }

    //按照count大小排序，取前N个
    val sortedItems = allitems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //释放内存空间
    itemState.clear()

    //输出排名结果
    val result:StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp-1)).append("\n")
    //输出每一个商品信息
    for(i<-sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No").append(i+1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }
    result.append("===================================")
    //控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}



