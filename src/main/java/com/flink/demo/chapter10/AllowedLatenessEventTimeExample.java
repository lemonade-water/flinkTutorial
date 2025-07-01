package com.flink.demo.chapter10;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;

/**
 * 允许迟到水位线示例
 * 迟到数据的处理三大招：
 * 第一招，设置允许迟到时间，默认是0，WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(允许迟到时间)
 * 第二招，允许迟到数据，.window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
 *                 .allowedLateness(Duration.ofSeconds(3))
 * 第三招，使用测输出流
 */
public class AllowedLatenessEventTimeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        OutputTag<WaterSensor> outputTag = new OutputTag<>("lateData", Types.POJO(WaterSensor.class));
        env.setParallelism(1);
        SingleOutputStreamOperator<String> process = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getVc() * 1000)
                                .withIdleness(Duration.ofSeconds(3)))
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                //设置允许迟到
                .allowedLateness(Duration.ofSeconds(3))
                //侧输出流 关窗的数据
                .sideOutputLateData(outputTag)
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
                });
        //打印测流
        process.print();
        process.getSideOutput(outputTag).printToErr();

        env.execute();
    }
}
