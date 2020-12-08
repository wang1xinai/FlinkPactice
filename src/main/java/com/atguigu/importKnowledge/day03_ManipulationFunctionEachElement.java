package com.atguigu.importKnowledge;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-02 15:05
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 处理函数：
 *         >可以访问事件的时间戳信息和水位线信息、注册定时事件、输出特定事件（eg.超时事件）
 *         >都继承自富函数，自动实现了open close方法
 * 8个ProcessFunction extends AbstractRichFunction implement RichFunction(都有open()、close()和getRuntimeContext()等方法):
 *          ~ ProcessFunction                                       dataDStream
 *          ~ KeyedProcessFunction<K, I, O>                         KeyedStream，来一条数据处理一次
 *          ~ CoProcessFunction<IN1, IN2, OUT>                      双流合并的时候使用。
 *          ~ ProcessJoinFunction<IN1, IN2, OUT>
 *          ~ BroadcastProcessFunction<IN1, IN2, OUT>
 *          ~ KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT>
 *          ~ ProcessWindowFunction<IN, OUT, KEY, W extends Window>  KeyedStream
 *          ~ ProcessAllWindowFunction<IN, OUT, W extends Window>    DataStream
 *
 * extends KeyedProcessFunction<K, I, O>
 *     KeyedProcessFunction处理KeyedStream,
 *          In: 流中的每一个数据
 *          Out: 0个 1个或多个元素
 *          def: processElement(v: IN, ctx: Context, out: Collector[OUT])
 *                  来一条数据就调用一次这个方法；
 *                  用collector向下游输出数据；...
 *                  用context修改内部状态/创建Timer...
 *               onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[OUT])
 *                  回调函数。当之前用TimerService注册的定时器触发时调用。
 */
public class day03_ManipulationFunctionEachElement {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 9999);
        hadoop102.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] s = value.split(" ");
                return Tuple2.of(s[0], Long.parseLong(s[1]));
            }
        })
                .keyBy(r -> r.f0)
                .process(new MyKeyed())
                .print();

        env.execute();
    }

//        <K, I, O>
//    O: out,这个函数最终计算的结果
    public static class MyKeyed extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

//        这个方法来一条数据处理触发一次这个方法
        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
//       获取当前机器时间
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
//       注册定时器
            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 15 * 1000L);
            out.collect("数据来了");
        }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        out.collect("定时事件触发了，时间戳是" + timestamp);
    }
}
}
