package com.flink.demo.chapter02;

import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(new WaterSensor("sensor_1", 1l, 1),
                        new WaterSensor("sensor_2", 1l, 2),
                        new WaterSensor("sensor_2", 1l, 2),
                        new WaterSensor("sensor_2", 1l, 2)
                        )
                .keyBy(data->data.id)
                .print();
        env.execute();
    }
}
