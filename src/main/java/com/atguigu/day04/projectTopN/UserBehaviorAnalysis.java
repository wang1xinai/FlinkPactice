package com.atguigu.day04.projectTopN;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author wangxin'ai
 * @Description // TODO TOP3 PV
 * @createDate 2020-12-04 19:55
 */

/**
    列表状态变量： private ListState<ItemViewCount> itemTop3List;
    状态变量有可能是存在状态后端的，比如说硬盘、HDFS，
        所以不能直接排序，要用一个ArraryList存储在内存后排序。

 Comparator:
    .sort(new Comparator<ItemViewCount>() {public compare{}}
 */

public class UserBehaviorAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> readTextFile = env.readTextFile("E:\\0621atguigu\\code\\idea\\FlinkToturial\\src\\main\\resources\\UserBehavior.csv");
        readTextFile.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                UserBehavior userBehavior = new UserBehavior();
                String[] split = value.split(",");
                userBehavior.UserId = split[0];
                userBehavior.itemID = split[1];
                userBehavior.categoryId = split[2];
                userBehavior.behavior = split[3];
                userBehavior.timestamp = Long.parseLong(split[4]) * 1000L;
                return userBehavior;
            }
        })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
//                        创建一个单调递增时间戳的水位线: 没有迟到的时间
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        //指定传入数据的哪个是时间戳
                                        return element.timestamp;
                                    }
                                })
                ).keyBy(r -> r.itemID)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(r->r.windowEnd)
                .process(new TopPVItem3(3))
                .print();

        env.execute();
    }

    //计算top3
    public static class TopPVItem3 extends KeyedProcessFunction<Long,ItemViewCount,String>{

        private Integer threshold;
        //存储top3的值状态变量
        private ListState<ItemViewCount> itemTop3List;

        public TopPVItem3(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemTop3List = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-stateTop3", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemTop3List.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Iterable<ItemViewCount> itemViewCounts = itemTop3List.get();
            ArrayList<ItemViewCount> arrayList = new ArrayList<>();
            for (ItemViewCount itemViewCount : itemViewCounts) {
                arrayList.add(itemViewCount);
            }
            itemTop3List.clear();
            arrayList.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount  o2) {
                    return (int)(o2.count.longValue() - o1.count.longValue());
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("=======================")
                    .append("窗口结束时间：")
                    .append(new Timestamp(timestamp -1))
                    .append("\n");

            for (Integer i = 0; i < threshold; i++) {
                ItemViewCount currItem = arrayList.get(i);
                result.append("No.")
                        .append(i+1)
                        .append(":")
                        .append(currItem.item)
                        .append("浏览量 = ")
                        .append(currItem.count)
                        .append("\n");
            }

            result.append("================");
            out.collect(result.toString());
        }
    }


    //计算每个item商品的PV
    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    //为（item,PV个数）封装上窗口信息，返回ItemViewCount对象
    public static class WindowResult extends ProcessWindowFunction<Long,ItemViewCount,String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<ItemViewCount> out) throws Exception {
            Long Count = elements.iterator().next();
            ItemViewCount itemViewCount = new ItemViewCount();
            itemViewCount.item = key;
            itemViewCount.count = Count;
            itemViewCount.windowStart = context.window().getStart();
            itemViewCount.windowEnd  =context.window().getEnd();
            out.collect(itemViewCount);
        }
    }
}
