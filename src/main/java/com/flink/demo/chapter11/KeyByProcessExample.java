package com.flink.demo.chapter11;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * keyby demo
 */
public class KeyByProcessExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1:1:7
        //2:1:7
        //3:1:7
        //4:1:7
        env.setParallelism(2);
        env.fromElements(
                        Tuple2.of("s1", 1),
                        Tuple2.of("s1", 2)
                ).returns(Types.TUPLE(Types.STRING, Types.INT))
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        return value != null;
                    }
                })
                .keyBy(data -> data.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Object>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                        //key process function
                    }
                });
        env.execute();
    }
}
