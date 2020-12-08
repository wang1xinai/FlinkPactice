package com.atguigu.day02;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-01 10:33
 */
public class SensorReading {

    public String id;
    public long timestamp;
    public double temperature;

    public SensorReading() { }

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }
}

