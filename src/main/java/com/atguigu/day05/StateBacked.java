package com.atguigu.day05;

import com.atguigu.day02.SensorSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangxin'ai
 * @Description // TODO
 * @createDate 2020-12-05 15:01
 */
//spark stage?划分stage,拍快照
/**
 * 容错机制：
 *      是什么：用检查点保存flink计算状态，出现故障时进行故障恢复的。
 *          检查点的实现算法：异步快照。Chandy-Lamport算法实现分布式快照（分别去做，最后再拼起来）；
 *                                  将检查点的保存和数据处理分开，不暂停整个应用。
 *                          检查点分界线（Checkpoint Barrier）:与watermark相似，在流中插入一个特殊的数据形式
 *                                  00000 | 0000 | 00000  0代表数据，|代表检查点分界线
 *                                   保存检查点分界线前最后一个数据流过flink JobGraph后，每个任务的状态。 保存检查点过程中，检查点分界线后面的数据会先被缓存，保存完检查点再计算。
 *      怎么用：
 *          配置：异步进行checkpoint的操作
                   env.getCheckpointConfig().setCheckpointInterval(1L*60*1000L);
                   env.setStateBackend(new FsStateBackend("file:///E:\\0621atguigu\\code\\idea\\FlinkToturial\\src\\main\\resources\\checkPointStateBacked"));
                   env.enableCheckpointing(1*60*1000L);
 *          存储检查点：JobManager存储元数据任务拓扑图，hdfs等外部存储存储检查点数据。
 *          读取检查点
 *          重启策略配置：
 *              env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.seconds(50)));
 *      有什么作用：
 *
 * 保存点：
 *      可以理解为是 带有元数据信息的检查点。
 *      保存点只能手动触发，而检查点是周期性自动存储
 *      保存点是一个强大的功能，除了做故障恢复外，还可以用于：· 有计划的手动备份
 *                                                      · 版本迁移、更新应用程序等。
 *
 *
 */

/**
    状态一致性：
        每一个算子任务的内部数据（？）都是任务的状态。所谓一致性，就是说计算结果要保证准确、正确。就算发生了故障，一条数据也不应该丢失，不应该重复计算。
        级别：at most once   可能丢失数据。     性能好
             at least once  不丢，可能重复。    UV
             exactly once   有且只有一次。      销售额
                实现： 检查点 —— flink内部状态一致性的保证
                      可重设数据的读取位置（eg.kafka）即可保证source数据不丢不重复 —— source
                      flink内部一致，可保证不会丢；幂等写入/事务写入可保证不会重复  ——sink端
                            幂等写入：一个操作重复执行多次，结果都是一样的。比如redis 的 hash数据结构
                            事务写入：原子性：要么全成功，要么全失败。flink中将checkpoint和sink写入绑定为一个事务。
                                     > 预写日志WAL：把结果保存到状态后端后，等收到checkpoint完成的通知后，再一次性写入sink
                                     > 两阶段提交2PC：sink下游必须支持事务支持。先进行“预提交”（），等待checkpoint保存成功后，再真正提交。提交事务必须是一个幂等操作
 ———端的状态一致性。kafka socket，取决于最弱的一环
            check point就是用来保证状态一致性和故障恢复的。
 */
public class StateBacked {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(1L*60*1000L);
        env.setStateBackend(new FsStateBackend("file:///E:\\0621atguigu\\code\\idea\\FlinkToturial\\src\\main\\resources\\checkPointStateBacked"));
        env.enableCheckpointing(1*60*1000L);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.seconds(50)));
        env.addSource(new SensorSource()).print();
        env.execute();
    }
}
