package com.atguigu.day02;

/**
 * @author wangxin'ai
 * @Description // TODO 练习 键控流转换算子
 * @createDate 2020-12-01 15:26
 */


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.redis.RedisCodecException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

/**
练习转换算子：
 1. 基本转换算子： map  fliter  flatMap
 2. 键控流转换算子: keyBy、 sum max maxBy等、reduce
 3. 多流转换算子
 4. 分布式转换算子

 滚动聚合：维护一个值（累加器），当流来了，根据流的数据更新这个值。
 */
public class RollingSumOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //keyBy sum
//        DataStream<Tuple3<Integer, Integer, Integer>> inputStream = env
//                .fromElements(new Tuple3(1, 2, 2), new Tuple3(2, 3, 1), new Tuple3(2, 2, 4), new Tuple3(1, 5, 3));
//
//        DataStream<Tuple3<Integer, Integer, Integer>> resultStream = inputStream
//                .keyBy(0) // key on first field of the tuple
//                .sum(1);   // sum the second field of the tuple in place
//        resultStream.print();

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
        sensorReadingDataStreamSource.filter(r->r.id.equals("sensor_1"))
                .map(new MapFunction<SensorReading, Tuple3<String,Double,Double>>() {
                    @Override
                    public Tuple3<String, Double, Double> map(SensorReading value) throws Exception {
                        return Tuple3.of(value.id,value.temperature,value.temperature);
                    }
                })
                .keyBy(0)
                //这里的reduce起的作用是：传出温度最小值和温度最大值。
                // 输出：（id,温度最小值，温度最大值）
                .reduce(new ReduceFunction<Tuple3<String, Double, Double>>() {
                    @Override
                    public Tuple3<String, Double, Double> reduce(Tuple3<String, Double, Double> value1, Tuple3<String, Double, Double> value2) throws Exception {
                        Tuple3<String, Double, Double> out = new Tuple3<>();
                        out.f0 = value1.f0;
                        if(value1.f1 > value2.f1){
                            out.f1 = value2.f1;
                        }else {
                            out.f1 = value1.f1;
                        }
                        if(value1.f2 > value2.f2){
                            out.f2 = value1.f2;
                        }else {
                            out.f2 = value2.f2;
                        }
                        return out;
                    }
                }).print();

        env.execute();
    }


}
