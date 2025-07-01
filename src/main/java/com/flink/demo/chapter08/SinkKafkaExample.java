package com.flink.demo.chapter08;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class SinkKafkaExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        SingleOutputStreamOperator<String> streamOperator = env.socketTextStream("hadoop102", 7777);

        /**
         * 如果精准一次，①必须设置事务超时时间: 大于checkpoint间；②必须设置事务ID前缀；③开启checkpoint
         *
         */
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.204.134:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("sink-kafka")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //  如果要使用事务，必须设置事务ID前缀
                .setTransactionalIdPrefix("hds-")
                // 如果是精准一次，必须设置 事务超时时间: 大于checkpoint间
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000+"")
                .build();

        streamOperator.sinkTo(sink);
        env.execute();
    }
}
