package com.flink.demo.chapter06;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 合流
 */
public class UnionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        DataStreamSource<Integer> s1 = executionEnvironment.fromElements(1, 2, 3, 4);
        DataStreamSource<Integer> s2 = executionEnvironment.fromElements(7,8,9,10);
        DataStreamSource<String> s3 = executionEnvironment.fromElements("12","13","14");
        s1.union(s2).union(s3.map(Integer::valueOf)).print();
        executionEnvironment.execute();
    }
}
