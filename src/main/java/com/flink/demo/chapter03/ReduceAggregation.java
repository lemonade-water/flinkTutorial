package com.flink.demo.chapter03;

import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 归约聚合 reduce
 *
 */
public class ReduceAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<WaterSensor, String> keyedStream = env.fromElements(
                        new WaterSensor("sensor_1", 1l, 1),
                        new WaterSensor("sensor_1", 1l, 2),
                        new WaterSensor("sensor_1", 9l, 4),
                        new WaterSensor("sensor_1", 2l, 4)
                ).keyBy(new KeySelector<WaterSensor, String>() {
                    @Override
                    public String getKey(WaterSensor waterSensor) throws Exception {
                        return waterSensor.id;
                    }
                });
        keyedStream.reduce(new ReduceFunction<WaterSensor>(){
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("Demo7_Reduce.reduce");
                int maxVc = Math.max(value1.getVc(), value2.getVc());
                //实现max(vc)的效果  取最大值，其他字段以当前组的第一个为主
                //value1.setVc(maxVc);
                //实现maxBy(vc)的效果  取当前最大值的所有字段
                if (value1.getVc() > value2.getVc()){
                    value1.setVc(maxVc);
                    return value1;
                }else {
                    value2.setVc(maxVc);
                    return value2;
                }
            }
        }).print();
        env.execute();
    }
}
