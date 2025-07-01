package com.flink.demo.chapter09;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class ThreeTimeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(new FilterFunction<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value != null;
                    }
                })
                .keyBy(WaterSensor::getId)
                //滚动窗口 10s
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                //滑动窗口 10s，5s
                //.window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10),  Duration.ofSeconds(5)))
                //会话窗口
                //.window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(5)))
                //会话窗口 动态提取器 每天数据来都会变化
//                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
//                    @Override
//                    public long extract(WaterSensor element) {
//                        return element.getTs() * 1000;
//                    }
//                }))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        LocalDateTime startDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(start), ZoneId.systemDefault());
                        LocalDateTime endDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(end), ZoneId.systemDefault());
                        out.collect("窗口开始时间：" + startDate + "，结束时间：" + endDate + "，大小：" + elements.spliterator().estimateSize());
                    }
                }).print();
        env.execute();
    }
}
