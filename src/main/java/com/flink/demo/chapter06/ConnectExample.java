package com.flink.demo.chapter06;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 73, 3);
        DataStreamSource<String> source2 = env.fromElements("4", "7", "0");

        ConnectedStreams<Integer, String> connect = source1.connect(source2);
        SingleOutputStreamOperator<String> process = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer integer) throws Exception {
                return integer.toString();
            }

            @Override
            public String map2(String s) throws Exception {
                return s;
            }
        });
        process.print();
        env.execute();
    }
}
