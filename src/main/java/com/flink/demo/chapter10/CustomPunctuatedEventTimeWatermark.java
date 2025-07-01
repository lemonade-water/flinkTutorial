package com.flink.demo.chapter10;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 自定义断点水位线
 */
public class CustomPunctuatedEventTimeWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(new FilterFunction<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) {
                        return value != null;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                            @Override
                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                                return new WatermarkGenerator<WaterSensor>(){
                                    private Long delayTime = 5000L; // 延迟时间
                                    private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时
                                    @Override
                                    public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
                                        maxTs = Math.max(event.getTimestamp(),maxTs);
                                        //断点式生成器会不停地检测 onEvent()中的事件，当发现带有水位线信息的事件时，就立
                                        //即发出水位线。我们把发射水位线的逻辑写在onEvent方法当中即可。
                                        output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                    }
                                };
                            }
                        })
                        .withTimestampAssigner((element, recordTimestamp) -> {
                            System.out.println(Instant.ofEpochMilli(element.getTimestamp())
                                    .atZone(ZoneId.systemDefault())
                                    .format(DateTimeFormatter.ofPattern("HH:mm:ss")));
                            return element.getTimestamp();
                        })
                        .withIdleness(Duration.ofSeconds(3)) //分区空闲 结束
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                //监控水位线
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        LocalDateTime startDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(start), ZoneId.systemDefault());
                        LocalDateTime endDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(end), ZoneId.systemDefault());
                        long l = elements.spliterator().estimateSize();
                        out.collect("窗口开始时间：" + startDate + "，结束时间：" + endDate + "key:" + s + ",元素个数：" + l);
                    }
                })
                .print();
        env.execute();
    }
}
