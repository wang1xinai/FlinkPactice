package com.atguigu.day03;

import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author wangxin'ai
 * @Description // TODO 增量聚合函数aggregation
 * @createDate 2020-12-02 11:55
 */

//窗口函数
//    概念：用timeWindow/countWindow/Window进行开窗口，对窗口中的数据如何进行操作，就是窗口函数
//    分类：
//          增量聚合函数（一个分区只维护一个累加器）
//          全窗口聚合函数（带有窗口信息）
//          增量聚合函数 + 全窗口聚合函数
//    API
public class WindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
        SingleOutputStreamOperator<SensorReading> sensor_1 = sensorReadingDataStreamSource.filter(r -> r.id.equals("sensor_1"));
        sensor_1
                .keyBy(r->r.id)
                .timeWindow(Time.seconds(5))
                .aggregate(new MyAgg())
                .print();

        env.execute();
    }

    public  static  class MyAgg implements AggregateFunction<SensorReading,Tuple2<String,Double>,Tuple2<String,Double>>{
//        求某个森林监控的传感器的最高温度值。

        @Override
        public Tuple2<String, Double> createAccumulator() {
            return Tuple2.of("",Double.MIN_VALUE);
        }

        @Override
        public Tuple2<String, Double> add(SensorReading value, Tuple2<String, Double> accumulator) {
            accumulator.f0 = value.id;
            if(accumulator.f1 < value.temperature){
                accumulator.f1 = value.temperature;
            }
            return accumulator;
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple2<String, Double> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Double> merge(Tuple2<String, Double> a, Tuple2<String, Double> b) {
            return null;
        }
    }
}
