package com.atguigu.importKnowledge;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wangxin'ai
 * @Description // TODO flink入门wordcount练习
 * @createDate 2020-11-30 12:11
 */


/**
 Flink的输入数据流；
        > 消息队列
        > 文件系统
        > 实时产生的（例如socket）。
 */
public class day01_wordcount {
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
