package day5

import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: day5
  * Version: 1.0
  *
  * Created by wushengran on 2020/4/21 11:45
  */
object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.socketTextStream("localhost", 7777)
    val dataStream: DataStream[SensorReading] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        SensorReading( dataArray(0), dataArray(1).toLong, dataArray(2).toDouble )
      } )

    env.execute("state test job")
  }
}

class MyProcessor extends KeyedProcessFunction[String, SensorReading, Int]{
//  lazy val myState: ValueState[Int] = getRuntimeContext.getState( new ValueStateDescriptor[Int]("my-state", classOf[Int]) )
  lazy val myListState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("my-liststate", classOf[String]))
  lazy val myMapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("my-mapstate", classOf[String], classOf[Double]))
  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading](
    "my-reducingstate",
    new MyReduceFunction(),
    classOf[SensorReading])
  )
  var myState: ValueState[Int] = _
  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState( new ValueStateDescriptor[Int]("my-state", classOf[Int]) )
  }

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
    myState.value()
    myState.update(1)
    myListState.add("hello")
    myListState.update(new util.ArrayList[String]())
    myMapState.put("sensor_1", 10.0)
    myMapState.get("sensor_1")
    myReducingState.add(value)
    myReducingState.clear()
  }
}

class MyReduceFunction() extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value1.timestamp.max(value2.timestamp), value1.temperature.min(value2.temperature))
}