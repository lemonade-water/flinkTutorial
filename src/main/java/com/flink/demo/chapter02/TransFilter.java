package com.flink.demo.chapter02;

import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1l, 1),
                new WaterSensor("sensor_2", 2l, 2)
        );
        stream.filter(data->"sensor_1".equals(data.getId())).print();
        env.execute();
    }
}
