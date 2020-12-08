package com.atguigu.day02.moke;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-01 18:44
 */
public class Alter {
    public String message;
    public long timestamp;

    public Alter() {
    }

    public Alter(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Alter{" +
                "message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
