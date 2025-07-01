package com.flink.demo.chapter04;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.fromElements("a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z")
                //.shuffle()
                //.rebalance()
                .rescale()
                .print();
        env.execute();
    }
}
