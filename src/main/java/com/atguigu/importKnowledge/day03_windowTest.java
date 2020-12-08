package com.atguigu.importKnowledge;

import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;

/**
 * @author wangxin'ai
 * @Description // TODO 练习窗口函数
 * @createDate 2020-12-02 10:33
 */

//窗口：当我们做BI统计/一段时间内的统计时，都需要窗口函数。一般是开窗 + 分组聚合的模式。
//    概念：其实就是将无限的数据流切割为有限范围的数据。无限->有限
//    分类: .window
//          时间TimeWindow:滚动、滑动、会话（flink独有）.timeWindow
//          计数CountWindow：滚动、滑动                 .countWindow

//    API
public class day03_windowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //通过窗口函数，计算每个传感器一段时间内温度的最低值。
        SingleOutputStreamOperator<Tuple3<String, String, Double>> mapStream = env.addSource(new SensorSource()).map(new MapFunction<SensorReading, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(SensorReading value) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String timeYymmdd = simpleDateFormat.format(value.timestamp);
                return Tuple3.of(value.id, timeYymmdd, value.temperature);
            }
        }).setParallelism(10).filter(r->r.f0.equals("sensor_1"));

        KeyedStream<Tuple3<String, String, Double>, String> keyedStream = mapStream.keyBy(r -> r.f0);
        keyedStream.timeWindow(Time.seconds(5)).reduce(new ReduceFunction<Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> reduce(Tuple3<String, String, Double> value1, Tuple3<String, String, Double> value2) throws Exception {
                Tuple3<String, String, Double> out = new Tuple3<>();
                out.f0 = value1.f0;
                out.f1 = value2.f1;
                if(value1.f2 > value2.f2){
                    out.f2 = value2.f2;
                }else{
                    out.f2 = value1.f2;
                }
                return out;
            }
        }).setParallelism(1).print();

        env.execute();
    }
}
