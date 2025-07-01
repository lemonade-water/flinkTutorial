package com.flink.demo.chapter11;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import com.flink.demo.pojo.WaterSensorVcCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * topN 问题
 */
public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forMonotonousTimestamps())
                .keyBy(WaterSensor::getVc)
                .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .aggregate(new AggregateFunction<WaterSensor, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(WaterSensor waterSensor, Long aLong) {
                        return aLong+1;
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
                    public void process(Integer integer, ProcessWindowFunction<Long, WaterSensorVcCount, Integer, TimeWindow>.Context context, Iterable<Long> elements, Collector<WaterSensorVcCount> out) throws Exception {
                        WaterSensorVcCount waterSensorVcCount = new WaterSensorVcCount();
                        waterSensorVcCount.setCount(elements.iterator().next());
                        waterSensorVcCount.setVc(integer);
                        out.collect(waterSensorVcCount);
                    }
                })
                //windowAll 的缺点：
                //windowAll 将所有数据（WaterSensorVcCount）收集到一个任务（Task），由单一 Task Manager 处理。
                //全局窗口的并行度为 1（parallelism = 1），无法利用 Flink 的分布式计算能力。
                //内存占用随数据量和窗口大小增长（如 10 秒内 100 万条记录）。
                //可能触发 GC（垃圾回收）或 OOM（内存溢出）。
                .windowAll(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .process(new ProcessAllWindowFunction<WaterSensorVcCount, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensorVcCount, String, TimeWindow>.Context context, Iterable<WaterSensorVcCount> elements, Collector<String> out) throws Exception {
                        // 收集所有 vc 计数
                        int n = 2;
                        List<WaterSensorVcCount> vcCounts = new ArrayList<>();
                        for (WaterSensorVcCount vcCount : elements) {
                            vcCounts.add(vcCount);
                        }

                        // 按计数降序排序，计数相同按 vc 升序
                        vcCounts.sort(Comparator.comparingLong(WaterSensorVcCount::getCount).reversed()
                                .thenComparingInt(WaterSensorVcCount::getVc));

                        // 取 Top N
                        StringBuilder result = new StringBuilder();
                        TimeWindow window = context.window();
                        result.append(String.format("Window: [%s, %s), Top %d: [", formatTime(window.getStart()), formatTime(window.getEnd()), n));
                        for (int i = 0; i < Math.min(n, vcCounts.size()); i++) {
                            WaterSensorVcCount vcCount = vcCounts.get(i);
                            result.append(String.format("(vc=%d, count=%d)", vcCount.getVc(), vcCount.getCount()));
                            if (i < Math.min(n, vcCounts.size()) - 1) {
                                result.append(", ");
                            }
                        }
                        result.append("]");
                        out.collect(result.toString());
                    }
                })
                .print("sink topN");
        env.execute();
    }

    private static String formatTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }
}
