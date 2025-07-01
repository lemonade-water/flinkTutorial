package com.flink.demo.chapter03;

import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单聚合算子
 *
 * sum max min maxby minby
 *
 */
public class SumAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> sum = env.fromElements(
                        new WaterSensor("sensor_1", 1l, 1),
                        new WaterSensor("sensor_2", 1l, 2),
                        new WaterSensor("sensor_3", 1l, 3),
                        new WaterSensor("sensor_1", 1l, 4),
                        new WaterSensor("sensor_2", 3l, 5),
                        new WaterSensor("sensor_3", 4l, 6))
                .keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return waterSensor.id;
                    }
                }).minBy("ts");
        sum.print();
        env.execute();
    }
}
