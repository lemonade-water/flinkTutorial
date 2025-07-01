package com.flink.demo.chapter12;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * 值状态管理示例：
 * 检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警。
 */
public class ValueSateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .returns(WaterSensor.class)
                .filter(Objects::nonNull)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private transient ValueState<WaterSensor> lastWaterSensorState;

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        ValueStateDescriptor<WaterSensor> declaration = new ValueStateDescriptor<>("lastWaterSensor", WaterSensor.class);
                        lastWaterSensorState = getRuntimeContext().getState(declaration);
                    }

                    @Override
                    public void processElement(WaterSensor currentWaterSensor, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        WaterSensor lastWaterSensor = lastWaterSensorState.value();
                        if (lastWaterSensor != null) {
                            if (currentWaterSensor.getVc() >= 10 && lastWaterSensor.getVc() >= 10){
                                out.collect(String.format("传感器%s的当前水位值%s和上一次水位值%s都超过10，请及时处理！",
                                        currentWaterSensor.getId(), currentWaterSensor.getVc(), lastWaterSensor.getVc()));
                            }
                        }
                        lastWaterSensorState.update(currentWaterSensor);
                    }
                }).print();
        env.execute();
    }
}
