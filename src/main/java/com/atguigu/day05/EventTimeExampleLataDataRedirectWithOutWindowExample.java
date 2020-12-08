package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.xml.bind.ValidationEvent;

/**
 * @author wangxin'ai
 * @Description // TODO 水位线
 * TODO 用侧输出处理迟到数据:不开窗侧输出
 * @createDate 2020-12-04 14:41
 */

/**
 *
 */

/***
 * 水位线：
 *      默认公式：水位线 = 观察到的事件携带的最大的时间戳 - 最大延时时间 - 1ms
 *               何为观察到的时间携带的最大时间戳？：就是输入到DStream中的所有数据中，时间戳最大的数据的时间戳。
 *      自定义水位线公式：
 *          1.有周期性规律
 *          .assignTimestampsAndWatermarks(
 *                         new AssignerWithPeriodicWatermarks<SensorReading>() {
 *                              private long maxTS=？;//设置最大时间戳的默认值。
 *                             //插入水位线的时候调用。new Watermark(更新逻辑/公式)
 *                             public getCurrentWatermark{}
 *                             //来一条数据调用一次。可以用来修改观察到的时间携带的最大的时间戳
 *                             public extractTimestamp{}
 *                         }
 *          2. 没有周期习性规律
 *  要求：
 *      要求数据源带有时间戳信息。
 *  使用：
 *      1. env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); 修改整条流使用事件时间，而不是处理时间
 *      2. datastram.assignTimestampsAndWatermarks(设置最大延迟时间、告知数据中哪个字段是数据的时间戳字段)向数据流中打入水位线属性，水位线跟随数据一起流动。
 *          WatermarkStrategy.
 *          WatermarkStrategy类别：
 *              forBoundedOutOfOrderness 数据乱序，有迟到的时间的数据
 *              forMonotonousTimestamps  数据单调递增，没有迟到的时间的数据。不需设置最大延迟时间
 *                  .withTimestampAssigner(new SerializableTimestampAssigner设置数据源中哪个字段是时间戳字段)
 *  更新：默认每200ms更新一次，可以配置：env.getConfig().setAutoWatermarkInterval(1000L*60); // 1000L millisecond = 1s
 *      更新的值是：
 *          系统会在最开始插入一个无穷小的数值作为水位线
 *          观察到的事件携带的最大的时间戳 - 最大延时时间 - 1ms。
 *  用处：判断是否关闭窗口，将水位线的值与各窗口的右边界值比较，当水位线>=右边边界值-1ms时，关闭对应的窗口。
 *        绝不仅仅是关闭窗口这一条作用？？？？
 *  watermark的设定：
 *      如何设定：先将业务数据直接输出到文本后，查看数据，估算最高延迟时间应该设置为多少。
 *      最高延迟时间设置的若太大，会占用大量内存：可以在水位线到达之前，先输出一个近似的结果。eg.trigger??
 *      最高延迟时间设置的若太小，可能数据不准确： 有以下三种处理方案：
 *          1. 直接丢弃
 *          2. 侧输出？         =>  推荐  *****
 *                开窗侧输出
 *                不开窗侧输出：dataDStream.process(new ProcessFunction{ processElement(){
         *                                          if (value.f1 < ctx.timerService().currentWatermark()) {
         *                                              ctx.output(value)
 *                                                      }
         *                                           }
 *                                              )
 *          3. 窗口输出结果后，不关闭窗口，迟到数据来了之后更新窗口数据。=> 不推荐，因为窗口会占用内存，保留窗口时间太长会占用太多内存。
 *
 *
 *      流的计算过程中，衡量事件进展只有一个标准：水位线
 *      每个多长时间插入一条水位线，由source的机器时间确定（默认200ms）。
 */
public class EventTimeExampleLataDataRedirectWithOutWindowExample {

    public static OutputTag<Tuple2<String, Long>> lastData = new OutputTag<Tuple2<String, Long>>("lastData") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置时间特性，比如：处理时间、事件时间或摄入时间。
        //修改整条流使用事件时间，而不是处理时间。默认水位线间隔时间为200ms
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> processStream = hadoop102
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value.split(" ")[0], Long.parseLong(value.split(" ")[1]) * 1000L);
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                }))
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if (value.f1 < ctx.timerService().currentWatermark()) {
                            out.collect("迟到的数据就是你！" + value.toString() + ",当前水位线是：" + ctx.timerService().currentWatermark());
                            ctx.output(lastData, value);
                        } else {
                            out.collect("没有迟到的数据" + value.toString() + ",当前水位线是：" + ctx.timerService().currentWatermark());
                        }
                    }
                });
        processStream.print();
        processStream.getSideOutput(lastData).print();
        env.execute();
    }
}
