package com.flink.demo.chapter11;


import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import com.flink.demo.pojo.WaterSensorVcCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 使用定时器的方式 模拟 滑动窗口
 */
public class TopNExample2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2); // 分布式计算

        DataStream<WaterSensor> sensorStream = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L));

        // 第一阶段：按 vc 分组，统计计数
        SingleOutputStreamOperator<WaterSensorVcCount> outputStreamOperator = sensorStream.keyBy(WaterSensor::getVc)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .aggregate(new AggregateFunction<WaterSensor, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }
                    @Override
                    public Long add(WaterSensor waterSensor, Long aLong) {
                        return aLong + 1;
                    }
                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }
                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return acc1 + aLong;
                    }
                }, new ProcessWindowFunction<Long, WaterSensorVcCount, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer vc, ProcessWindowFunction<Long, WaterSensorVcCount, Integer, TimeWindow>.Context context, Iterable<Long> elements, Collector<WaterSensorVcCount> out) throws Exception {
                        WaterSensorVcCount waterSensorVcCount = new WaterSensorVcCount();
                        waterSensorVcCount.setCount(elements.iterator().next());
                        waterSensorVcCount.setVc(vc);
                        waterSensorVcCount.setWindowEnd(context.window().getEnd());
                        out.collect(waterSensorVcCount);
                    }
                });
        outputStreamOperator
                .keyBy(WaterSensorVcCount::getWindowEnd)
                .process(new KeyedProcessFunction<Long, WaterSensorVcCount, String>() {
                    private Map<Long,List<WaterSensorVcCount>> map = new HashMap<>();

                    private final Integer TOP_N_NUM = 2;

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, WaterSensorVcCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        TimeDomain timeDomain = ctx.timeDomain();
                        if (timeDomain.equals(TimeDomain.EVENT_TIME)){
                            //处理事件时间
                            //排序 取值
                            //out.collect("ProcessingTime Timer fired " +  " at " + formatTime(timestamp));
                            List<WaterSensorVcCount> list = map.get(ctx.getCurrentKey());
                            list.sort(new Comparator<WaterSensorVcCount>() {
                                @Override
                                public int compare(WaterSensorVcCount o1, WaterSensorVcCount o2) {
                                    return o2.getCount().compareTo(o1.getCount());
                                }
                            });
                            for (int i = 0; i < Math.min(TOP_N_NUM, list.size()); i++) {
                                WaterSensorVcCount vcCount = list.get(i);
                                out.collect(String.format("time:%s Top %d: (vc=%d, count=%d)",formatTime(timestamp), i + 1 , vcCount.getVc(), vcCount.getCount()));
                            }
                        }
                    }

                    @Override
                    public void processElement(WaterSensorVcCount value, KeyedProcessFunction<Long, WaterSensorVcCount, String>.Context ctx, Collector<String> out) throws Exception {
                        if (map.containsKey(ctx.getCurrentKey())) {
                            map.get(ctx.getCurrentKey()).add(value);
                        } else {
                            // 1.1 不包含vc，是该vc的第一条，需要初始化list
                            List<WaterSensorVcCount> dataList = new ArrayList<>();
                            dataList.add(value);
                            map.put(ctx.getCurrentKey(), dataList);
                        }
                        //设置定时器
                        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1000L);
                    }
                }).print();
        env.execute("Top N Water Level Timer No WindowAll");
    }
    private static String formatTime(long time) {
        Instant instant = Instant.ofEpochMilli(time);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
        return formatter.format(instant);
    }
}