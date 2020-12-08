package com.atguigu.day05;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-05 13:14
 */

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 双流join :
 * <p>
 * union     DataStream.  两条数据类型一样的流进行join,机器时间。
 *              应用：比如当数据源的数据量极大时，将flink source的并行度调高，然后再join，进行下一步处理。
 * connect   DataStream.  无关数据join,机器时间
 *              应用：比如将数据流2 的数据作为数据流1 的数据向下游流动的开关。
 * interval  KeyedStream. 基于间隔的join。
 *              应用：比如：点击动作日志 join 一个小时之内的浏览动作日志
 */

public class TwoDstreamJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<Tuple3<String, Long, String>, String> stream1 = env
                .fromElements(
                        Tuple3.of("user_1", 10 * 60 * 1000L, "click"),
                        Tuple3.of("user_2", 10 * 60 * 1000L, "click")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, String>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, String> stringLongStringTuple3, long l) {
                                        return stringLongStringTuple3.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0);

        KeyedStream<Tuple3<String, Long, String>, String> stream2 = env
                .fromElements(
                        Tuple3.of("user_1", 5 * 60 * 1000L, "browse"),
                        Tuple3.of("user_1", 6 * 60 * 1000L, "browse"),
                        Tuple3.of("user_2", 10 * 60 * 1000L, "browse")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, String>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, String> stringLongStringTuple3, long l) {
                                        return stringLongStringTuple3.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0);

        stream1
                .intervalJoin(stream2)
                .between(Time.minutes(-10), Time.minutes(0))
                .process(new ProcessJoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, String>() {
                    @Override
                    public void processElement(Tuple3<String, Long, String> stringLongStringTuple3, Tuple3<String, Long, String> stringLongStringTuple32, Context context, Collector<String> collector) throws Exception {
                        collector.collect(stringLongStringTuple3 + " => " + stringLongStringTuple32);
                    }
                })
                .print();

        env.execute();
    }
}
