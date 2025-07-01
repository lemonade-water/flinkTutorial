package com.flink.demo.chapter11;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * 定时器
 */
public class KeyByProcessWithTimerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getVc() * 1000))
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    // 状态：记录每个用户的最新事件时间
                    private transient ValueState<Long> lastEventTime;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态
                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("lastEventTime", Long.class);
                        lastEventTime = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(WaterSensor event, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 更新状态
                        lastEventTime.update(event.getVc() * 1000L);

                        // 注册事件时间定时器（事件时间 + 5 秒）0
                        long eventTimer = event.getVc() * 1000L + 5000L;
                        ctx.timerService().registerEventTimeTimer(eventTimer);
                        out.collect(String.format("Registered EventTime Timer for user %s at %s (fires at %s), current watermark: %s",
                                event.getId(), formatTime(event.getVc() * 1000L), formatTime(eventTimer),
                                formatTime(ctx.timerService().currentWatermark())));

//                        // 注册处理时间定时器（当前时间 + 5 秒）
//                        long processingTimer = ctx.timerService().currentProcessingTime() + 5000L;
//                        ctx.timerService().registerProcessingTimeTimer(processingTimer);
//                        out.collect(String.format("Registered ProcessingTime Timer for user %s at %s (fires at %s)",
//                                event.getId() , formatTime(ctx.timerService().currentProcessingTime()), formatTime(processingTimer)));
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 获取当前键（用户 ID）
                        String userId = ctx.getCurrentKey();
                        Long lastTime = lastEventTime.value();

                        // 判断定时器类型
                        if (ctx.timeDomain() == TimeDomain.EVENT_TIME) {
                            out.collect(String.format("EventTime Timer triggered for user %s at %s, last event time: %s",
                                    userId, formatTime(timestamp), lastTime != null ? formatTime(lastTime) : "null"));
                        } else if (ctx.timeDomain() == TimeDomain.PROCESSING_TIME) {
                            out.collect(String.format("ProcessingTime Timer triggered for user %s at %s, last event time: %s",
                                    userId, formatTime(timestamp), lastTime != null ? formatTime(lastTime) : "null"));
                        }
                    }
                }).print();
        env.execute();
    }

    private static String formatTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }
}
