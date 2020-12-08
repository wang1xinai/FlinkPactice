package com.atguigu.day02;

import jdk.nashorn.internal.ir.Flags;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-01 10:36
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    public boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random rand = new Random();

        String[] sensorIds = new String[10];
        double[] CurFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + i;
            CurFTemp[i] = 65+(rand.nextGaussian() *20);
        }

        while (running){
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                CurFTemp[i] +=rand.nextGaussian() * 0.5;
                ctx.collect(new SensorReading(sensorIds[i],curTime, CurFTemp[i]));
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
