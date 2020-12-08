package com.atguigu.day04;

import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author wangxin'ai
 * @Description // TODO 侧输出
 * 需求：将传感器温度小于30度的输出到一个流，
 * 将传感器温度大于100度的输出到另一个流。
 * @createDate 2020-12-04 9:33
 */

/**
 * 侧输出：将流中不同的数据发送到其他流中。分流
 * 用法：.process(new ProcessFunction) + 侧输出标签
 *          ctx.output(侧输出流的名)
 *       .getSideOutput(侧输出流的名).print()
 */
public class MySideOutput {

    public static OutputTag<String> output1 = new OutputTag<String>("LessThan30F") {
    };
    public static OutputTag<String> output2 = new OutputTag<String>("MoreThan100F") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> process = sensorReadingDataStreamSource
                .keyBy(r -> r.id)
                .process(new MySideOutputFunc());

        process.getSideOutput(output1).print();
        process.getSideOutput(output2).print();
        env.execute();
    }

    public static class MySideOutputFunc extends ProcessFunction<SensorReading, SensorReading> {

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            if (value.temperature < 30) {
                ctx.output(output1, "温度小于30度的流");
        }
            if (value.temperature > 100) {
                ctx.output(output2, "温度大于100度的流");
            }

            out.collect(value);
        }
    }
}
