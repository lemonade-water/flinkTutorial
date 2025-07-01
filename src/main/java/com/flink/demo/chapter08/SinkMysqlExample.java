package com.flink.demo.chapter08;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkMysqlExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("hadoop102", 7777).map(new WaterSensorMapFunction());

        String sql = "INSERT INTO ws (id, ts, vc) VALUES (?, ?, ?)";
        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://192.168.204.134:3306/flink-sink?useSSL=false&serverTimezone=UTC")
                //.withDriverName("com.mysql.cj.jdbc.Driver")
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                //重点 检查连接超时
                .withConnectionCheckTimeoutSeconds(60)
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(100) // 批量写入大小
                .withBatchIntervalMs(2000) // 批量写入间隔
                .withMaxRetries(3) // 重试次数
                .build();

        // 4. 创建 JDBC Sink
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink(
                sql,
                (JdbcStatementBuilder<WaterSensor>) (preparedStatement, waterSensor) -> {
                    preparedStatement.setString(1, waterSensor.getId());
                    preparedStatement.setLong(2, waterSensor.getTs());
                    preparedStatement.setInt(3, waterSensor.getVc());
                },
                executionOptions,
                connectionOptions
        );
        streamOperator.addSink(jdbcSink);
        // 6. 执行任务
        env.execute("Sink to MySQL Example");

    }
}
