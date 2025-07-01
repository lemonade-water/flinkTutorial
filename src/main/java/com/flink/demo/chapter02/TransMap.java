package com.flink.demo.chapter02;

import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1l, 1),
                new WaterSensor("sensor_2", 2l, 2)
        );

        //写法1
        //stream.map(data -> data.id).print();

        //写法2
        stream.map(new MapFunction<WaterSensor, Object>() {
            @Override
            public Object map(WaterSensor waterSensor) throws Exception {
                return waterSensor.id;
            }
        }).print();
        env.execute();
    }
}
