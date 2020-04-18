package com.atguigu.apitest

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/18 14:13
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading( dataArray(0), dataArray(1).toLong, dataArray(2).toDouble )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream = dataStream
      .keyBy("id")
//      .window( EventTimeSessionWindows.withGap(Time.minutes(1)) )    // 会话窗口
//      .window( TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)) )
      .timeWindow(Time.seconds(15), Time.seconds(5))
//      .reduce( new MyReduce() )
      .apply( new MyWindowFun() )

    dataStream.print("data")
    resultStream.print("result")
    env.execute("window test")
  }
}

// 自定义一个全窗口函数
class MyWindowFun() extends WindowFunction[SensorReading, (Long, Int), Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(Long, Int)]): Unit = {
    out.collect((window.getStart, input.size))
  }
}