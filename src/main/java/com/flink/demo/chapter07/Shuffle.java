package com.flink.demo.chapter07;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 7777);
        //suffle是随机的
        //stream.shuffle().print();

        //
        env.execute();
    }
}
