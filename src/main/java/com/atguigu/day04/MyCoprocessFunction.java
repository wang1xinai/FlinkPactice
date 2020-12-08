package com.atguigu.day04;

/**
 * @author wangxin'ai
 * @Description // TODO 练习Coprocessfunction。
 * TODO 需求：通过流2数据决定流1数据是否继续向下流动。
 * 碰到第一个流2 的数据后，就让流1输出，输出流2中指定的时间后停止输出。
 * @createDate 2020-12-04 9:04
 */

import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * CoProcessFunction:
 *  IN：ConnectDStream
 *  OUT: 自己控制
 * 作用：对双流合并后的数据进行处理。
 * 用法: .connect().process(new CoProcessFunction())
 * <p>
 * 其他：> 值变量作为流2 控制 流1的开关
 *      > 定时器控制时间
 */
public class MyCoprocessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
        DataStreamSource<Tuple2<String, Long>> fromElements = env.fromElements(Tuple2.of("sensor_1", 10 * 1000L));
        ConnectedStreams<SensorReading, Tuple2<String, Long>> connect = sensorReadingDataStreamSource.keyBy(r -> r.id).connect(fromElements.keyBy(r -> r.f0));
        connect.process(new MyCoprocessFunction2()).print();

        env.execute();
    }

    public static class MyCoprocessFunction2 extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {
        public ValueState<Boolean> enableForwarding;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            enableForwarding = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("switch", Types.BOOLEAN));
        }

        @Override
        public void processElement1(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if (enableForwarding.value() != null && enableForwarding.value()) {
                out.collect(value);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<SensorReading> out) throws Exception {
            enableForwarding.update(true);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + value.f1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            enableForwarding.clear();
        }
    }
}
