package com.atguigu.day06;

import com.atguigu.day02.SensorSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-07 11:15
 */
public class SinkKafkaProducerExactlyOnceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> hello_world_kafka = env.fromElements("hello world kafka");
        hello_world_kafka.addSink(new FlinkKafkaProducer<String>(
                "hadoop102:9092,hadoop103:9092,hadoop104:9092",
                "hello_world_topic",
                new SimpleStringSchema()));

        env.execute();
    }
}
