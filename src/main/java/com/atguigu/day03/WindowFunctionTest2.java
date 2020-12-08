package com.atguigu.day03;

import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import javafx.beans.binding.DoubleExpression;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @author wangxin'ai
 * @Description // TODO 全窗口聚合函数
 * @createDate 2020-12-02 11:55
 */

//窗口函数
//    概念：用timeWindow/countWindow/Window进行开窗口，对窗口中的数据如何进行操作，就是窗口函数
//    分类：
//          增量聚合函数（一个分区只维护一个累加器）
//          全窗口聚合函数（带有窗口信息）process(new MyProcess())
//          增量聚合函数 + 全窗口聚合函数
//    API

//    processFunction和processWindowFunction的区别是？
public class WindowFunctionTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
//        SingleOutputStreamOperator<SensorReading> sensor_1 = sensorReadingDataStreamSource.filter(r -> r.id.equals("sensor_1"));
        sensorReadingDataStreamSource
                .keyBy(r->r.id)
                .timeWindow(Time.seconds(5))
                .process(new MyProcess())
                .print();

        env.execute();
    }

    //泛型：
    //      W  这个窗口是什么类型的窗口：时间窗口/计数窗口
    //这个类的作用：记录监控某个森林的传感器的温度 某个窗口 最大值，将这个最大值传递到下一个任务。
    public static  class MyProcess extends ProcessWindowFunction<SensorReading, String, String, TimeWindow> {
        /**
         * @author wxa_zf
         * @Description //TODO
         * @Date 13:46 2020/12/2
         * @Param [ s：The key for which this window is evaluated.窗口的key
         *          context： The context in which the window is being evaluated.窗口的上下文信息
         *          elements：一个周期的窗口的所有数据。是一个迭代器。
         *          out：要向下游发送的内容。
         *        ]
         * @return void
         */
        @Override
        public void process(String s, Context context, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
//          在一个时间周期窗口关闭时会调用这个方法
            double minValue = Double.MIN_VALUE;
            for (SensorReading element : elements) {
                if(element.temperature > minValue){
                    minValue = element.temperature;
                }
            }
            String startTime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(context.window().getStart());
            String endTime = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(context.window().getEnd());
            out.collect("传感器ID为"+s+"的森林，在"
                    +startTime+"~~~"+endTime
                    +"时间周期的最高温度是 "+minValue);
        }
    }
}
