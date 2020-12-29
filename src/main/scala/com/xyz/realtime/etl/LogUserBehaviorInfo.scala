package com.xyz.realtime.etl

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.{DataStreamSource, DataStreamUtils}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object LogUserBehaviorInfo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("comsumer_log_bevavior", "test001")
    properties.setProperty("bootstrap.servers", "172.31.64.169:9092,172.31.65.150:9092,172.31.65.232:9092," +
      "172.31.65.74:9092,172.31.66.161:9092,172.31.66.1:9092,172.31.66.28:9092,172.31.66.74:9092")
    val ds: DataStreamSource[String] = env.addSource(new FlinkKafkaConsumer[String]("online_data_001", new SimpleStringSchema(), properties))
    //stream.print()
    val res = DataStreamUtils.collect(ds)
    println(res)
    //println(ds)
    //env.execute()
  }
}

