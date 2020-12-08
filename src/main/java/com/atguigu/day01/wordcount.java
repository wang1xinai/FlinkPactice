package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

/**
 * @author wangxin'ai
 * @Description // TODO flink入门wordcount练习
 * @createDate 2020-11-30 12:11
 */

/**
 *  什么是flink: 对有限流和无限流数据源 进行 流式有状态处理 的分布式计算引擎。
 *      特点： 事件时间、处理时间；精确一致性；低延迟；上下游存储系统支持多；高可用、动态扩展，7*24小时全天候运行
 *  为什么要用flink: 最接近真实现实环境中生产数据的方式；
 *                  能够做到低延迟、高吞吐、并且还能保证结果正确、也具有良好的容错性。
 *        flink = 高吞吐+压力下保证正确(spark streaming 微批次5s) +
 *                低延迟（storm） +
 *                时间语义正确 + 操作简单、表现力好 ...
 *
 *  分层API: SQL/TABLE API
 *           DataStream API  数据流API，主要是无界流；可获得流的信息，可开窗。用DataSet API做有界流
 *           ProcessFunction 自定义状态、时间语义（事件时间/处理时间）、定时器等。以上的所有操作，都可以用ProcessFunction实现

 spark 开窗：跨越多个采集周期
 flink 开窗：无界流变为有界流
 */
public class wordcount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取数据源
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        source.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String s, Collector<WordCount> collector) throws Exception {
                String[] strings = s.split(" ");
                for (String string : strings) {
                    collector.collect(new WordCount(string,1));
                }
            }
        }).keyBy(r->r.word).reduce(new ReduceFunction<WordCount>() {
            @Override
            public WordCount reduce(WordCount value1, WordCount value2) throws Exception {
                return new WordCount(value1.word, value1.count + value2.count);
            }
        }).print();

        env.execute("wordcount");
    }

    public static class WordCount{
        public String word;
        public Integer count;

        public WordCount() {
        }

        public WordCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
