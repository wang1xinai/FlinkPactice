package com.atguigu.day06;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-07 11:15
 */
public class SinkKafkaConsumerExactlyOnceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties kafkaproperties = new Properties();
        kafkaproperties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");

        env.addSource(new FlinkKafkaConsumer<String>(
                "hello_world_topic",
                new SimpleStringSchema(),
                kafkaproperties
        )).print();

        env.execute();
    }
}
