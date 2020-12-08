package com.atguigu.day02;

import com.atguigu.day02.moke.Alter;
import com.atguigu.day02.moke.SmokeLevelSource;
import com.atguigu.day02.moke.smokeLevel;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunctionNewtonForm;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author wangxin'ai
 * @Description // TODO 练习多流转换算子
 * @createDate 2020-12-01 18:15
 */

/**
 练习转换算子：
 1. 基本转换算子： map  fliter  flatMap
 2. 键控流转换算子: keyBy、 sum max maxBy等、reduce
 3. 多流转换算子：union connect
 4. 分布式转换算子
 */
public class multistreamConversionOperatorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

//        1. union
//        DataStreamSource<String> sds1 = env.fromElements("1", "2", "3");
//        DataStreamSource<String> sds2 = env.fromElements("4", "5", "6");
//
//        sds1.union(sds2).print();

//        双流join：当森林温度监控传感器 和 森林烟雾监控传感器 都达到阈值时，报警
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
        DataStreamSource<smokeLevel> smokeLevelDataStreamSource = env
                .addSource(new SmokeLevelSource())
                .setParallelism(1);

        //将温度监控传感器流 按传感器id分到不同的slot中并行执行
        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = sensorReadingDataStreamSource.keyBy(r -> r.id);
        sensorReadingStringKeyedStream
                //双流join。
                // 将烟雾传感器的数据变为广播变量，每个taskManager上的所有task共同访问一个内存，节省空间。
                .connect(smokeLevelDataStreamSource.broadcast())
                //判断二者是否达到阈值，如果达到阈值，输出报警信息。现在先输出到控制台
                .flatMap(new AlterFlatMap())
                .print();

        env.execute();

    }

    public static class AlterFlatMap implements CoFlatMapFunction<SensorReading, smokeLevel, Alter>{

        //判断二者是否达到阈值，如果达到阈值，输出报警信息。现在先输出到控制台

        public smokeLevel SmokeLevel = smokeLevel.LOW; //森林最开始的状态肯定是没有着火的状态。烟雾报警器处于正常值。
        @Override
        public void flatMap1(SensorReading value, Collector<Alter> out) throws Exception {
            //当当前流入的数据是 温度传感器的数据时，执行flatMap1这个方法
//            value :(id, timestamp, temperature)
            if (value.temperature >100 && this.SmokeLevel == smokeLevel.HIGH){
                out.collect(new Alter("传感器ID为：" + value.id +"的森林着火了！",value.timestamp));
            }
        }

        @Override
        public void flatMap2(smokeLevel value, Collector<Alter> out) throws Exception {
            //当当前流入的数据是 烟雾传感器的数据时，执行flatMap2这个方法
           this.SmokeLevel = value;
        }
    }

}
