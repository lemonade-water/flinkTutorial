package com.flink.demo.chapter09;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class FullWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        WindowedStream<WaterSensor, String, TimeWindow> window = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(new FilterFunction<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value != null;
                    }
                }).keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Duration.of(10, ChronoUnit.SECONDS)));
//        window.apply(new WindowFunction<WaterSensor, String, String, TimeWindow>() {
//            /**
//             *
//             * @param s The key for which this window is evaluated.
//             * @param window The window that is being evaluated.
//             * @param input The elements in the window being evaluated.
//             * @param out A collector for emitting elements.
//             * @throws Exception
//             */
//            @Override
//            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out) throws Exception {
//
//            }
//        }).print();
        window.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            /**
             *
             * @param s 分组的key
             * @param context 上下文
             * @param elements The elements in the window being evaluated.
             * @param out 收集器
             * @throws Exception
             */
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long start = context.window().getStart();
                long end = context.window().getEnd();
                LocalDateTime startDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(start), ZoneId.systemDefault());
                LocalDateTime endDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(end), ZoneId.systemDefault());
                System.out.println("窗口开始时间：" + startDate + "，结束时间：" + endDate);
                long l = elements.spliterator().estimateSize();
                System.out.println("key:" + s + ",元素个数：" + l);
            }
        });
        env.execute();
    }
}
