package com.flink.demo.chapter01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamSocketWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //env.setParallelism(1);
        // 2. 读取文本流：hadoop102表示发送端主机名、7777表示端口号
        DataStreamSource<String> lineStream = env.socketTextStream("hadoop102", 7777);
        // 3. 转换、分组、求和，得到统计结果
        SingleOutputStreamOperator<Tuple2<String, Long>> sum =
                lineStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                            String[] words = line.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        })
                        .setParallelism(1)
                        .returns(Types.TUPLE(Types.STRING, Types.LONG))
                        .keyBy(data -> data.f0)
                        .sum(1);

        // 4. 打印
        sum.print();
        // 5. 执行
        env.execute();
    }
}
