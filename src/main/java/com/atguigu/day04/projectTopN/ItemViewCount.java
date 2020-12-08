package com.atguigu.day04.projectTopN;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-04 20:29
 */
//POJO
public class ItemViewCount {
    public String item;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

    public ItemViewCount() {
    }

    public ItemViewCount(String item, Long count, Long windowStart, Long windowEnd) {
        this.item = item;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "item='" + item + '\'' +
                ", count=" + count +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
