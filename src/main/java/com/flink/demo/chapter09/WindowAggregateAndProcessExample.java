package com.flink.demo.chapter09;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WindowAggregateAndProcessExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<WaterSensor, String> keyby = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(new FilterFunction<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value != null;
                    }
                }).keyBy(WaterSensor::getId);
        keyby.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor waterSensor, Integer integer) {
                        return waterSensor.getVc()+integer;
                    }

                    @Override
                    public String getResult(Integer integer) {
                        return integer.toString();
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        //调用完aggregate完，会调用process方法
                        System.out.println("调用process");
                        out.collect("key="+s+"窗口:"+context.window()+"数据:"+elements+"结果:"+elements.iterator().next());
                    }
                }).print();
        env.execute();
    }
}
