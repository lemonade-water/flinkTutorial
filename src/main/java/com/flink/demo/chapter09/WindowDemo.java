package com.flink.demo.chapter09;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        WaterSensorMapFunction waterSensorMapFunction = new WaterSensorMapFunction();
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("hadoop102", 7777).map(waterSensorMapFunction);
        KeyedStream<WaterSensor, String> keyedStream = streamOperator.keyBy(WaterSensor::getId);

        //todo 1.指定窗口分配器 指定 用 哪一种 窗口 ：时间 or 计算？ 滚动 滑动 会话 全局窗口？
        //keyedStream.windowAll() 非按键分区
        //keyedStream.window() 按键分区
        //窗口分配器 时间窗口 滚动窗口
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        //滑动窗口
        //keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
        //会话窗口
        //keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))

        //基于计数
        //keyedStream.countWindow(5);
        //keyedStream.countWindow(5,2);

        //todo 2.指定窗口操作
        //增量聚合：来一条数据，计算一条聚合结果，窗口触发的时候，结果输出
        keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((a, b) -> new WaterSensor(a.getId(), a.getTs(), a.getVc() + b.getVc()))
                .print();

        //全窗口函数：数据来了不处理，存起来，窗口触发的时候，计算结果输出
        //keyedStream.process()

        env.execute();
    }
}
