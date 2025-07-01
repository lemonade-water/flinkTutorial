package com.flink.demo.chapter10;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;
import java.util.Objects;

public class WindowJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1:1:7
        //2:1:7
        //3:1:7
        //4:1:7
        env.setParallelism(2);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                        Tuple2.of("s1", 1),
                        Tuple2.of("s1", 2),
                        Tuple2.of("s1", 3),
                        Tuple2.of("s1", 4)
                ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        return value!=null;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                        Tuple3.of("s1", 1, 7),
                        Tuple3.of("s1", 11, 8),
                        Tuple3.of("s1", 15, 9),
                        Tuple3.of("s1", 6, 0)
                ).returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT)) //显示返回
                .filter(new FilterFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, Integer, Integer> value) throws Exception {
                        return value!=null;
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1 * 1000L));

        //window join 类似inner join
        //flink 不建议使用window join 按照窗口固定
        ds1.join(ds2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .apply((left, right) -> left.f1 + ":" + right.f1 + ":" + right.f2,org.apache.flink.api.common.typeinfo.Types.STRING)
                .print();
        env.execute();
    }
}
