package com.flink.demo.chapter05;

import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        try {
            String[] parts = value.split(",");
            if (parts.length != 3) {
                System.out.println("Invalid input: " + value);
                return null; // 或抛出异常
            }
            return new WaterSensor(parts[0], Long.parseLong(parts[1]), Integer.parseInt(parts[2]));
        } catch (Exception e) {
            System.out.println("Error parsing input: " + value + ", error: " + e.getMessage());
            return null; // 或跳过
        }
    }
}
