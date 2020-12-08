package com.atguigu.day03;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-02 9:43
 */


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 练习转换算子：
 1. 基本转换算子： map  fliter  flatMap
 2. 键控流转换算子: keyBy、 sum max maxBy等、reduce
 3. 多流转换算子：union connect
 4. 分布式转换算子: shuffle(random策略) rebalance rescale(round-robin策略)
                   broadcast global custom
 */

//分布式转换算子的作用：控制或自定义数据所在的分区。
//          解决数据倾斜问题；
//          或根据需求将不同的数据发送到不同的分区，进行后续处理。

public class DistributedConversionOperatorTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

    }
}
