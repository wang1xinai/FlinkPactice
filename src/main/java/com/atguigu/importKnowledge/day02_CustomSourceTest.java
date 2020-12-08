package com.atguigu.importKnowledge;

import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangxin'ai
 * @Description // TODO 自定义输入流
 * @createDate 2020-12-01 10:49
 */
public class day02_CustomSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
        //基本转换算子练习：map、flatMap、fliter
        //三种方式书写：
        //              ->匿名函数，
        //              继承MapFunction，
        //              写一个类继承MapFunction；
//        map算子:
//        sensorReadingDataStreamSource.map(r->r.id).print();
//        sensorReadingDataStreamSource.map(new MapFunction<SensorReading, String>() {
//            @Override
//            public String map(SensorReading value) throws Exception {
//                return value.id;
//            }
//        }).print();
//        sensorReadingDataStreamSource.map(new MyMapFunction()).print();

//        fliter
//        sensorReadingDataStreamSource.filter(r -> r.temperature>20).print();
        sensorReadingDataStreamSource.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading value) throws Exception {
                String s = value.id.split("_")[1];
                char[] chars = s.toCharArray();
                int a = 0;
                for (char aChar : chars) {
                    int data = (int)aChar -48;
                    a += data;
                }
                if (a>5){
                    return false;
                }else{
                    return true;
                }
            }
        }).print();

//        DataStreamSource<String> stringDataStreamSource = env.fromElements("white", "black", "gray");
//        stringDataStreamSource.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out) throws Exception {
//                if ("white".equals(value)){
//                    out.collect(value);
//                }else if("gray".equals(value)){
//                    out.collect(value);
//                    out.collect(value);
//                }
//            }
//        }).print();
//        flatMap
//        sensorReadingDataStreamSource.print();

        env.execute("CustomSourceTest");

    }
    public static class MyMapFunction implements MapFunction<SensorReading,String>{

        @Override
        public String map(SensorReading value) throws Exception {
            return value.id;
        }
    }
}
