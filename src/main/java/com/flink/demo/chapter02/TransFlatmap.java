package com.flink.demo.chapter02;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransFlatmap {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("hello world", "hello flink")
                .flatMap((String line, Collector<String> out) -> {
                    String[] fields = line.split(" ");
                    for (String field : fields) {
                        out.collect(field);
                    }
                }).print();
        env.execute();
    }
}
