package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.apache.flink.util.OutputTag;

import javax.lang.model.util.ElementScanner6;
import javax.xml.stream.events.EndElement;

/**
 * @author wangxin'ai
 * @Description // TODO 水位线
 * TODO 处理迟到数据：允许数据迟到，到一定时间先输出窗口结果，但不关闭窗口，等迟到数据到来后，更新窗口结果
 * @createDate 2020-12-04 14:41
 * <p>
 * 值状态、列表状态、窗口状态
 */

/**
 * 值状态、列表状态、窗口状态
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
 *                开窗侧输出： .sideOutputLateData(lastData)
 .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {正常数据的计算}
 *                不开窗侧输出：dataDStream.process(new ProcessFunction{ processElement(){
 *                                                    if (value.f1 < ctx.timerService().currentWatermark()) {
 *                                                        ctx.output(value)
 *                                               }})
 *             processStream.getSideOutput(lastData).print(); //查看侧输出数据
 *          3. 窗口输出结果后，不关闭窗口，迟到数据来了之后更新窗口数据。=> 不推荐，因为窗口会占用内存，保留窗口时间太长会占用太多内存。
 *
 *
 *      流的计算过程中，衡量事件进展只有一个标准：水位线
 *      每个多长时间插入一条水位线，由source的机器时间确定（默认200ms）。
 */
public class EventTimeExampleLataAllowLatenessExample {

    public static OutputTag<Tuple2<String, Long>> lastData = new OutputTag<Tuple2<String, Long>>("lastData") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置时间特性，比如：处理时间、事件时间或摄入时间。
        //修改整条流使用事件时间，而不是处理时间。默认水位线间隔时间为200ms
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 9999);
        hadoop102
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
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                //水位线高于窗口最大时间，触发窗口计算后，5秒中之后关闭窗口
                .allowedLateness(Time.seconds(8))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        //当水位线超过窗口结束时间时，触发窗口计算
                        //用isUpdate标记窗口是否已经触发计算
                        ValueState<Boolean> isUpdate = context.windowState().getState(new ValueStateDescriptor<Boolean>("isUpdate", Types.BOOLEAN));

                        //计算当前窗口的元素个数
                        long size = 0L;
                        for (Tuple2<String, Long> element : elements) {
                            size += 1;
                        }
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        if (isUpdate.value() == null) {
                            //水位线超过窗口最大时间，并且窗口没有被计算过，触发窗口计算
                            isUpdate.update(true);
                            out.collect(start + "~~" + end + "窗口中的元素的个数为：" + size);
                        } else if(isUpdate.value()){
                            //isUpdate!=null && isUpdate=true，说明窗口已经被计算过
                            out.collect("又捕捉到一条超时数据！:" + elements.toString() + "窗口中元素的个数是:" + size);
                        }
                    }
                }).print();
        env.execute();
    }
}
