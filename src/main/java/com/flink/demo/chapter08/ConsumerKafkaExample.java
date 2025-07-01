package com.flink.demo.chapter08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConsumerKafkaExample {
    public static void main(String[] args) throws Exception {
        //消费kafka数据
// 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置 Kafka 消费者
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.204.134:9092") // Kafka 服务器地址
                .setTopics("sink-kafka") // 替换为你的 Kafka 主题
                .setGroupId("flink-consumer-group") // 消费者组 ID
                .setStartingOffsets(OffsetsInitializer.earliest()) // 从最早偏移量开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 反序列化器（假设消息是字符串）
                .build();

        // 3. 从 Kafka 读取数据
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 4. 处理数据（这里简单打印）
        stream.print();

        // 5. 执行 Flink 作业
        env.execute("Flink Kafka Consumer Example");

    }
}
