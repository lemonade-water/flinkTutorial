package com.flink.demo.chapter12;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 在map算子中计算数据的个数
 */
public class OperationListExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.socketTextStream("hadoop102", 7777)

                .map(new MyCountMapFunction())
                .print();
        env.execute();
    }
}
