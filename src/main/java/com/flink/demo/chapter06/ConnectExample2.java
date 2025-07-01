package com.flink.demo.chapter06;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ConnectExample2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 =
                env.fromElements(
                        Tuple2.of(1, "a1"),
                        Tuple2.of(1, "a2"),
                        Tuple2.of(2, "b"),
                        Tuple2.of(3, "c")
                );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 =
                env.fromElements(
                        Tuple3.of(1, "aa1", 1),
                        Tuple3.of(1, "aa2", 2),
                        Tuple3.of(2, "bb", 1),
                        Tuple3.of(3, "cc", 1)
                );
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);
        // 多并行度下，需要根据 关联条件 进行keyby，才能保证key相同的数据到一起去，才能匹配上
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectKey = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);
        SingleOutputStreamOperator<String> result = connectKey.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                Integer id = value.f0;
                // TODO 1.来过的s1数据，都存起来
                if (!s1Cache.containsKey(id)) {
                    // 1.1 第一条数据，初始化 value的list，放入hashmap
                    List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                    s1Values.add(value);
                    s1Cache.put(id, s1Values);
                } else {
                    // 1.2 不是第一条，直接添加到 list中
                    s1Cache.get(id).add(value);
                }

                //TODO 2.根据id，查找s2的数据，只输出 匹配上 的数据
                if (s2Cache.containsKey(id)) {
                    for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                        collector.collect("s1:" + value + "<--------->s2:" + s2Element);
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context context, Collector<String> collector) throws Exception {
                Integer id = value.f0;
                // TODO 1.来过的s2数据，都存起来
                if (!s2Cache.containsKey(id)) {
                    // 1.1 第一条数据，初始化 value的list，放入hashmap
                    List<Tuple3<Integer, String, Integer>>
                            s2Values = new ArrayList<>();
                    s2Values.add(value);
                    s2Cache.put(id, s2Values);
                } else {
                    // 1.2 不是第一条，直接添加到 list中
                    s2Cache.get(id).add(value);
                }

                //TODO 2.根据id，查找s1的数据，只输出 匹配上 的数据
                if (s1Cache.containsKey(id)) {
                    for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                        collector.collect("s1:" + s1Element + "<--------->s2:" + value);
                    }
                }
            }
        });
        result.print();

        env.execute();
    }
}
