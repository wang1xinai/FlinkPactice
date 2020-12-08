package com.atguigu.importKnowledge;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-02 9:43
 */


import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 练习转换算子：
 1. 基本转换算子： map  filter  flatMap
            flatMap也特别强大，能够实现map、filter
 2. 键控流转换算子: keyBy、 sum max maxBy等、reduce
            reduce是所有滚动聚合的泛化实现，也即所有的滚动聚合算子都可以通过reduce实现
 3. 多流转换算子：union connect
 4. 分布式转换算子: shuffle(random策略) rebalance rescale(round-robin策略)
                   broadcast global custom


 滚动聚合：维护一个值（累加器），当流来了，根据流的数据更新这个值。
 */

//分布式转换算子的作用：控制或自定义数据所在的分区。
//          解决数据倾斜问题；
//          或根据需求将不同的数据发送到不同的分区，进行后续处理。

public class day03_DistributedConversionOperatorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        

        env.execute();
    }
}
