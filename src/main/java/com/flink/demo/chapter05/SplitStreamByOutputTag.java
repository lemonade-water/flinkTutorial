package com.flink.demo.chapter05;

import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 侧输出流
 */
public class SplitStreamByOutputTag {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());

        OutputTag<WaterSensor> sensor1 = new OutputTag<>("sensor1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> sensor2 = new OutputTag<>("sensor2", Types.POJO(WaterSensor.class));

        SingleOutputStreamOperator<WaterSensor> streamOperator = ds.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, WaterSensor>.Context context, Collector<WaterSensor> collector) throws Exception {
                String id = waterSensor.getId();
                if ("s1".equals(id)) {
                    context.output(sensor1, waterSensor);
                } else if ("s2".equals(id)) {
                    context.output(sensor2, waterSensor);
                } else {
                    //主流
                    collector.collect(waterSensor);
                }
            }
        });
        streamOperator.print("主流");
        streamOperator.getSideOutput(sensor1).print("s1");
        streamOperator.getSideOutput(sensor2).print("s2");
        env.execute();
    }
}
