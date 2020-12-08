package com.atguigu.importKnowledge;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-02 16:17
 */


import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 需求1： 两次温度值如果超过一定限度就报警
 * 需求2：温度在1s之内连续上升就报警
 */
public class day03_ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
        sensorReadingDataStreamSource.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return Tuple2.of(value.id, value.temperature);
            }
        })
                .keyBy(r -> r.f0)
                .process(new MyProcessAlter())
                .print();

        env.execute();
    }

    //    <K, I, O>
    public static class MyProcessAlter extends KeyedProcessFunction<String, Tuple2<String, Double>, String> {
        //        值状态变量
//        每一个分组的key维护一个值状态变量，用于记录温度最大值
        public ValueState<Double> lastTemp;

        //        值状态变量
//        用于保存定时器的时间戳
        public ValueState<Long> timeTS;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE));
            timeTS = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timeTS", Types.LONG)
            );
        }

        @Override
        public void processElement(Tuple2<String, Double> value, Context ctx, Collector<String> out) throws Exception {

//            获取值状态
            Double preTemp;
            if(lastTemp.value() ==null){
                preTemp = 0.0;
            }else {
                preTemp = lastTemp.value();
            }
            Long preTimeTs;
            if(timeTS.value() == null){
                preTimeTs = 0L;
            }else{
                preTimeTs = timeTS.value();
            }

//            不要忘记更新温度状态。
            lastTemp.update(value.f1);

            if(value.f1 > preTemp && preTimeTs ==0L){
                //如果当前温度大于上一次温度且温度定时器不存在时，注册定时器Timer,更新时间
                long currenTime = ctx.timerService().currentProcessingTime();
                //注册定时器并修改时间状态的内容
                long alterTs = currenTime + 1000L;
                ctx.timerService().registerProcessingTimeTimer(alterTs);
                //？？为什么要加1000L
                timeTS.update(alterTs);
            }else if( preTemp==0.0 || value.f1 < preTemp){
                //如果这是流进来的第一个数据， 或者 这次的温度低于之前的温度，删除定时器Timer,清空时间状态timeTs
                ctx.timerService().deleteProcessingTimeTimer(preTimeTs);
                timeTS.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器ID为" + ctx.getCurrentKey() + "温度持续1s都在上升！！");
        }
    }
}
