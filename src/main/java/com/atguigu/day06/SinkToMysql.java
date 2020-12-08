package com.atguigu.day06;

import com.atguigu.day02.SensorReading;
import com.atguigu.day02.SensorSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-07 15:09
 */
public class SinkToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new SensorSource());
        sensorReadingDataStreamSource.addSink(new MYJDBCSink());


        env.execute();
    }

    public static class MYJDBCSink extends RichSinkFunction<SensorReading>{
        private Connection conn;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //获取连接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/sensor",
                    "root",
                    "123456");

            //建立插入语句、更新语句占位符
            insertStmt = conn.prepareStatement("INSERT INTO temps (id, temp) VALUES (?, ?)");
            updateStmt = conn.prepareStatement("UPDATE temps set temp = ? WHERE id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            //业务操作
            updateStmt.setDouble(1,value.temperature);
            updateStmt.setString(2,value.id.toString());
            updateStmt.setString(2,value.id.toString());
            updateStmt.execute();

            if (updateStmt.getUpdateCount() == 0){
                insertStmt.setString(1,value.id.toString());
                insertStmt.setDouble(2,value.temperature);
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            conn.close();
            insertStmt.close();
            updateStmt.close();
        }
    }
}
