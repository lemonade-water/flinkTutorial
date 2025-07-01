package com.flink.demo.chapter08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class SinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<String>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "Number"+aLong;
            }
        },Long.MAX_VALUE,RateLimiterStrategy.perSecond(1000),Types.STRING);
        // 必须开启checkpoint，否则一直都是 .inprogress
        //并行度 控制 多少文件正在写入
        env.setParallelism(2);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<String> dataStreamSource = env.fromSource(stringDataGeneratorSource, WatermarkStrategy.noWatermarks(), "source");
        //输出到文件系统
        // 4. 定义 StreamingFileSink
        StreamingFileSink<String> fileSink = StreamingFileSink
                .<String>forRowFormat(new Path("F:\\workpace\\flinkTutorial\\sink"),  // 输出目录（可以是 HDFS 路径，如 "hdfs://namenode:8020/output"）
                        new SimpleStringEncoder<String>("UTF-8")  // 字符串编码方式
                )
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartSuffix(".log")
                        .withPartPrefix("flink-sink")
                        .build())
                .withRollingPolicy(  // 滚动策略（控制文件何时关闭并生成新文件）
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))  // 每隔5分钟滚动一次
                                .withInactivityInterval(Duration.ofMinutes(1))  // 1分钟无新数据则滚动
                                .withMaxPartSize(1024 * 1024)
                                .build()
                )
                .build();
        dataStreamSource.addSink(fileSink).name("legacy-file-sink");
        env.execute();
    }
}
