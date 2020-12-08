package com.atguigu.importKnowledge;

import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @author wangxin'ai
 * @Description // TODO 增量聚合函数 + 全窗口函数相结合，计算 某个传感器 在 一段时间周期 的温度最大值
 * @createDate 2020-12-02 11:55
 */

//窗口函数
//    概念：用timeWindow/countWindow/Window进行开窗口，对窗口中的数据如何进行操作，就是窗口函数
//    分类：
//          增量聚合函数aggregation(来一条数据处理一条数据，最终输出累加器)（一个分区只维护一个累加器）
//                  .timeWindow(Time.seconds(5)).aggregate(new MyAgg())
//                  implements AggregationFunction<IN, ACC, OUT>
//          全窗口聚合函数(处理这个窗口的所有数据element)（带有窗口信息）
//                  .timeWindow(Time.seconds(5)).process(new MyProcess())
//                  extends ProcessWindowFunction<IN, OUT, KEY, W extends Window>
//          增量聚合函数 + 全窗口聚合函数
//                  .timeWindow(Time.seconds(5)).aggregate(new MyAgg(),new MyWindowFun())
//                   这个时候，全窗口聚合函数包装在增量聚合函数的外面，获得增量聚合函数的计算结果后，包装上窗口的信息。
//                  数据流动的路程是：element --agg--> accumulator --processWindownFunction--> 全窗口聚合函数的输出
//    API

//    processFunction和processWindowFunction的区别是？
public class day03_WindowFunctionTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
//        SingleOutputStreamOperator<SensorReading> sensor_1 = sensorReadingDataStreamSource.filter(r -> r.id.equals("sensor_1"));
        sensorReadingDataStreamSource
                .keyBy(r->r.id)
                .timeWindow(Time.seconds(5))
                .aggregate(new MyAgg(),new MyWindowFun())
                .print();

        env.execute();
    }

//    传入accumulator,包装上窗口的信息后，输出。
//    可以包装的商品信息有：起止时间
//    <IN, OUT, KEY, W extends Window>
    public static class MyWindowFun extends ProcessWindowFunction<Tuple2<String, Double>,String,String,TimeWindow>{

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Double>> elements, Collector<String> out) throws Exception {
        Tuple2<String, Double> temparetor = elements.iterator().next();
        String startTime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(context.window().getStart());
        String endTime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(context.window().getEnd());
        out.collect("传感器ID为"+s+"的森林，在"
                +startTime+"~~~"+endTime
                +"时间周期的最高温度是 "+temparetor.f1);
    }
}

//    AggregateFunction<IN, ACC, OUT>
    public static class MyAgg implements AggregateFunction<SensorReading, Tuple2<String,Double>,Tuple2<String, Double>>{


    @Override
    public Tuple2<String, Double> createAccumulator() {
        return Tuple2.of("",Double.MIN_VALUE);
    }

    @Override
    public Tuple2<String, Double> add(SensorReading value, Tuple2<String, Double> accumulator) {
        accumulator.f0 = value.id;
        if (accumulator.f1 < value.temperature){
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
