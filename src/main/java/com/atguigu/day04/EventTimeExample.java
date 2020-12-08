package com.atguigu.day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.lang.model.util.ElementScanner6;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author wangxin'ai
 * @Description // TODO 事件时间
 * TODO 为何引入事件事件？ 解决 数据发生/采集数据的时间 和 进行数据分析统计时间 不一样的场景
 * @createDate 2020-12-04 14:41
 */

/***
 * 水位线：
 *      默认公式：水位线 = 观察到的事件携带的最大的时间戳 - 最大延时时间 - 1ms
 *               何为观察到的时间携带的最大时间戳？：就是输入到DStream中的所有数据中，时间戳最大的数据的时间戳。
 *      自定义水位线公式：
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
 *      最高延迟时间设置的若太小，可能数据不准确：侧输出？
 *
 *      流的计算过程中，衡量事件进展只有一个标准：水位线
 *      每个多长事件插入一条水位线，由source的机器时间确定。
 */
public class EventTimeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置时间特性，比如：处理时间、事件时间或摄入时间。
        //修改整条流使用事件时间，而不是处理时间。默认水位线间隔时间为200ms
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L*60); // 1000L millisecond = 1s

        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 9999);
        hadoop102
                .map(r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1])*1000L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                //分配时间戳和水位线()
                //forBoundedOutOfOrderness是指进来的数据是乱序的,

                //使用.assignTimestampsAndWatermarks(设置最大延迟时间、告知数据中哪个字段是数据的时间戳字段)向数据流中打入水位线属性，水位线跟随数据一起流动。
                // 水位线每200ms更新一次，更新的值是：观察到的事件携带的最大的时间戳 - 最大延时时间 - 1ms。
                //水位线 = 观察到的事件携带的最大的时间戳 - 最大延时时间 - 1ms
                //水位线的用处是：判断是否关闭窗口，将水位线的值与各窗口的右边界值比较，当水位线=右边边界值-1时，关闭对应的窗口。

                .assignTimestampsAndWatermarks(
//                forBoundedOutOfOrderness创建一个事件无序的水位线，最大延迟时间设置为5s
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
//                                        告诉程序，时间戳是哪一个字段
                                        return element.f1;
                                    }
                                })
                ).keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        //统计窗口中有几条数据
                        long count = 0L;
                        for (Tuple2<String, Long> element : elements) {
                            count += 1;
                        }

                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long currentWatermark = context.currentWatermark();
                        String waterMarkDatetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(currentWatermark);
                        out.collect(new Timestamp(start) + "~~" + new Timestamp(end) + "的窗口中有：" + count + "个数据, waterMark是：" + waterMarkDatetime);
//                        out.collect(start + "~~" + end + "的窗口中有：" + count + "个数据, waterMark是：" + waterMarkDatetime);
                    }
                }).print();

        env.execute();
    }
}
