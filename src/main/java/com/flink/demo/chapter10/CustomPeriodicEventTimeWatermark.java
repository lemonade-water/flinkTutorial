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
 * 自定义周期性水位线
 */
public class CustomPeriodicEventTimeWatermark {
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
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        // 发射水位线，默认200ms调用一次 修改方法：env.getConfig().setAutoWatermarkInterval(400L);
                                        output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
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
                .keyBy(WaterSensor::getId);

        env.execute();
    }
}
