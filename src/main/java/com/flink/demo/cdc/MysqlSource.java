package com.flink.demo.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.DecimalFormat;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class MysqlSource {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        //当访问hdfs文件时候 需要使用访问用户名，否则报错：Permission denied: user=xxxx, access=WRITE, inode="/":root:supergroup:drwxr-xr-x
        System.setProperty("H" +
                "ADOOP_USER_NAME", "root");
        //检查点触发时间
        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(6));
        //检查点模式
        config.set(CheckpointingOptions.CHECKPOINTING_CONSISTENCY_MODE, CheckpointingMode.AT_LEAST_ONCE);
        //检查点存储方式
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        //检查点存储目录
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://hadoop102:8020/mysql-cdc-chk");
        //检查点最大并发
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
        //检查点超时时间
        config.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(1000));
        //检查点最小间间隔
        config.set(CheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofMillis(500));
        //检查点超时重试次数
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        //bigdecimal 序列化问题
        Map<String,Object> jsonConfig = new HashMap<>();
        jsonConfig.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        JsonDebeziumDeserializationSchema jdd = new JsonDebeziumDeserializationSchema(false, jsonConfig);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.204.134")
                .port(3306)
                .databaseList("mydb")
                .tableList("mydb.products")
                .username("root")
                .password("123456")
                .serverId("5401")
                .deserializer(jdd)
                .includeSchemaChanges(true)
                .startupOptions(StartupOptions.latest())
                .build();

        SinkFunction<String> jdbcSink = JdbcSink.sink(
                "INSERT INTO `products` (`id`, `name`, `code`, `price`, `remark`) VALUES (?, ?, ?, ?, ?)",
                (statement, record) -> {
                    // 假设 record 是 JSON 格式，需解析
                    // 这里简化为直接插入，实际需根据 JSON 结构解析
                    try {
                        // 打印 record 以调试 JSON 结构
                        System.out.println("Received record: " + record);
                        JSONObject json = JSONObject.parseObject(record);
                        System.out.println("json:"+json);
                        JSONObject data = json.getJSONObject("after");
                        if (data != null) {
                            // 检查 id 是否存在且非空
                            if (data.containsKey("id")) {
                                statement.setInt(1, data.getIntValue("id"));
                            } else {
                                throw new IllegalArgumentException("Missing or null 'id' in record: " + record);
                            }
                            statement.setString(2, data.getString("name"));
                            statement.setString(3, data.getString("code"));
                            // 处理 price 字段
                            String priceStr = data.getString("price");
                            if (priceStr != null && !priceStr.trim().isEmpty()) {
                                try {
                                    statement.setBigDecimal(4, new BigDecimal(priceStr));
                                } catch (NumberFormatException e) {
                                    statement.setBigDecimal(4, null); // 表允许 price 为 null
                                }
                            } else {
                                statement.setBigDecimal(4, null);
                            }
                            statement.setString(5, data.getString("remark"));
                        } else {
                            System.out.println("Skipping record with null 'after' field: "+ record);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to process record: " + record, e);
                    }
                },
                JdbcExecutionOptions.builder().withBatchSize(1000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.204.134:3307/mydb?useUnicode=true&characterEncoding=utf8")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        );

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .addSink(jdbcSink)
                .setParallelism(1);

        env.execute("MySQL CDC to MySQL Slave");
    }
}
