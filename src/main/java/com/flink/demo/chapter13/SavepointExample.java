package com.flink.demo.chapter13;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class SavepointExample {
    public static void main(String[] args) throws Exception {
        //检查点
        Configuration config = new Configuration();
        //当访问hdfs文件时候 需要使用访问用户名，否则报错：Permission denied: user=xxxx, access=WRITE, inode="/":root:supergroup:drwxr-xr-x
        System.setProperty("HADOOP_USER_NAME", "root");
        //检查点触发时间
        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(6));
        //检查点模式
        config.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.EXACTLY_ONCE);
        //检查点存储方式
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        //检查点存储目录
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://hadoop102:8020/chk");
        //检查点最大并发
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
        //检查点超时时间
        config.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(1000));
        //检查点最小间间隔
        config.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofMillis(500));
        //检查点超时重试次数
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
        //开启外部持久化存储（enableExternalizedCheckpoints）
        //用于开启检查点的外部持久化，而且默认在作业失败的时候不会自动清理，如果想释放
        //空间需要自己手工清理。里面传入的参数ExternalizedCheckpointCleanup指定了当作业取消的
        //时候外部的检查点该如何清理。
        //DELETE_ON_CANCELLATION：在作业取消的时候会自动删除外部检查点，但是如果
        //是作业失败退出，则会保留检查点。
        //RETAIN_ON_CANCELLATION：作业取消的时候也会保留外部检查点。
        config.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION, ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION);
        //检查点连续失败次数（
        config.set(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER, 2);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.socketTextStream("hadoop102", 7777)
                .uid("socket-1").name("socket-1")
                .map(new WaterSensorMapFunction())
                .uid("map-1").name("map-1")
                .returns(WaterSensor.class)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private transient ValueState<WaterSensor> lastWaterSensorState;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        ValueStateDescriptor<WaterSensor> declaration = new ValueStateDescriptor<>("lastWaterSensor", WaterSensor.class);
                        lastWaterSensorState = getRuntimeContext().getState(declaration);
                    }

                    @Override
                    public void processElement(WaterSensor currentWaterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        WaterSensor lastWaterSensor = lastWaterSensorState.value();
                        if (lastWaterSensor != null) {
                            if (currentWaterSensor.getVc() >= 10 && lastWaterSensor.getVc() >= 10) {
                                out.collect(String.format("传感器%s的当前水位值%s和上一次水位值%s都超过10，请及时处理！",
                                        currentWaterSensor.getId(), currentWaterSensor.getVc(), lastWaterSensor.getVc()));
                            }
                        }
                        lastWaterSensorState.update(currentWaterSensor);
                    }
                })
                .uid("process-1").name("process-1")
                .print()
                .uid("print-1").name("print-1");
        env.execute();
    }
}
