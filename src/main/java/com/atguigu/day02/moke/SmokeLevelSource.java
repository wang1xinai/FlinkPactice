package com.atguigu.day02.moke;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.omg.CORBA.PUBLIC_MEMBER;

import java.awt.geom.FlatteningPathIterator;
import java.util.Random;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-01 18:30
 */
public class SmokeLevelSource implements SourceFunction<smokeLevel> {

    public boolean running = true;

    @Override
    public void run(SourceContext<smokeLevel> ctx) throws Exception {
        //创造随机数，作为数据源传递给flink
        Random random = new Random();
        while (running){
            if(random.nextGaussian() >0.8){
                ctx.collect(smokeLevel.HIGH);
            }else {
                ctx.collect(smokeLevel.LOW);
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
