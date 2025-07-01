package com.flink.demo.chapter12;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * 列表状态管理：
 * 针对每种传感器输出最高的3个水位值
 */
public class ListStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction(), Types.POJO(WaterSensor.class))
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps())
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    private transient ListState<WaterSensor> listState;

                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        Iterator<WaterSensor> iterator = elements.iterator();
                        List<WaterSensor> top3List = new ArrayList<>();

                        while (iterator.hasNext()) {
                            WaterSensor next = iterator.next();
                            if (top3List.size() < 3) {
                                top3List.add(next);
                            } else {
                                // 获取最小值
                                top3List.stream().min(new Comparator<WaterSensor>() {
                                    @Override
                                    public int compare(WaterSensor o1, WaterSensor o2) {
                                        return o1.getVc() - o2.getVc();
                                    }
                                }).ifPresent(min -> {
                                    if (min.getVc() > next.getVc()) {
                                        top3List.remove(min);
                                        top3List.add(next);
                                    }
                                });
                            }
                        }
                        Iterable<WaterSensor> curIterable = listState.get();
                        Iterator<WaterSensor> curIterator = curIterable.iterator();
                        while (curIterator.hasNext()) {
                            WaterSensor next = curIterator.next();
                            if (top3List.size() < 3) {
                                top3List.add(next);
                            } else {
                                // 获取最小值
                                top3List.stream().min(new Comparator<WaterSensor>() {
                                    @Override
                                    public int compare(WaterSensor o1, WaterSensor o2) {
                                        return o1.getVc() - o2.getVc();
                                    }
                                }).ifPresent(min -> {
                                    if (min.getVc() < next.getVc()) {
                                        top3List.remove(min);
                                        top3List.add(next);
                                    }
                                });
                            }
                        }
                        listState.update(top3List);
                        StringBuilder sb = new StringBuilder("【");
                        for (WaterSensor waterSensor : top3List) {
                            sb.append(waterSensor.getVc()).append(" ");
                        }
                        sb.append("】");
                        out.collect("窗口[" + context.window().getStart() + "~" + context.window().getEnd() + ") 最高3个水位值：" + sb.toString());
                    }

                    @Override
                    public void open(OpenContext openContext) throws Exception {
                        ListStateDescriptor<WaterSensor> listStateDescriptor = new ListStateDescriptor<>("listState", WaterSensor.class);
                        listState = getRuntimeContext().getListState(listStateDescriptor);
                    }
                })
                .print();

        env.execute();
    }
}
