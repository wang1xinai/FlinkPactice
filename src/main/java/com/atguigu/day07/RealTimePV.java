package com.atguigu.day07;

import com.atguigu.day04.projectTopN.UserBehavior;
import com.sun.org.apache.xml.internal.serializer.utils.SerializerMessages_zh_CN;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-08 14:47
 */
public class RealTimePV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> textFile = env.readTextFile("E:\\0621atguigu\\code\\idea\\FlinkToturial\\src\\main\\resources\\UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> userBehaviorWithWaterMark = textFile.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split("，");
                return new UserBehavior(split[0], split[1], split[2], split[3], Long.parseLong(split[4]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));


        userBehaviorWithWaterMark.filter(r->r.behavior.equals("pv"))
                                 .map(r-> Tuple2.of("pv",1L))
                                 .print();
        env.execute();
    }
}
