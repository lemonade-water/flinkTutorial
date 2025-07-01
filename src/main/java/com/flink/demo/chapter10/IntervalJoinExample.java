package com.flink.demo.chapter10;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 只能基于事件时间窗口
 */
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1:1:7
        //2:1:7
        //3:1:7
        //4:1:7
        env.setParallelism(2);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                        Tuple2.of("s1", 1), //-1 3
                        Tuple2.of("s1", 2) // 0 4
                ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        return value != null;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                        Tuple3.of("s1", 1, 7),
                        Tuple3.of("s1", 3, 8),
                        Tuple3.of("s1", 6, 0),
                        Tuple3.of("s1", -3, 0)
                ).returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT)) //显示返回
                .filter(new FilterFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, Integer, Integer> value) throws Exception {
                        return value != null;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1 * 1000L));


        //分别做key by ，key by就是关联条件
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(t -> t.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(t -> t.f0);
        //
        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2)
                .between(Duration.ofSeconds(-2), Duration.ofSeconds(2))
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        //进入这个方法表示关联上的
                        out.collect("left:" + left + " right:" + right);
                    }
                });
        process.print();
        env.execute();
    }
}
