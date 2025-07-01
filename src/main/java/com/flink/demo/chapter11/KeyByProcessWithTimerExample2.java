package com.flink.demo.chapter11;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class KeyByProcessWithTimerExample2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getVc() * 1000))
                .keyBy(WaterSensor::getId)
                        .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                            private transient ValueState<Long> lastEventTime;
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("lastEventTime", Long.class);
                                lastEventTime = getRuntimeContext().getState(descriptor);
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                //触发定时器
                                String key = ctx.getCurrentKey();
                                TimeDomain timeDomain = ctx.timeDomain();
                                //两个不一样的时间域
                                if (timeDomain.equals(TimeDomain.EVENT_TIME)){

                                }else if (timeDomain.equals(TimeDomain.PROCESSING_TIME)){

                                }
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                lastEventTime.update(value.getVc() * 1000L);

                                //获取定时器
                                TimerService timerService = ctx.timerService();
                                //设置处理时间定时器
                                timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() +  5000L);
                                out.collect(String.format("Registered ProcessingTime Timer for user %s at %s (fires at %s)",
                                        value.getId(), formatTime(timerService.currentProcessingTime()), formatTime(timerService.currentProcessingTime() + 5000L)));

                                //设置事件时间定时器
                                timerService.registerEventTimeTimer(value.getVc() * 1000L + 5000L);
                                out.collect(String.format("Registered EventTime Timer for user %s at %s (fires at %s),cur watermark at %s",
                                        value.getId(), formatTime(value.getVc() * 1000L), formatTime(value.getVc() * 1000L + 5000L),formatTime(timerService.currentWatermark())));
                            }

                            private String formatTime(long timestamp) {
                                return Instant.ofEpochMilli(timestamp)
                                        .atZone(ZoneId.systemDefault())
                                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                            }
                        }).print();
        env.execute();
    }
}
