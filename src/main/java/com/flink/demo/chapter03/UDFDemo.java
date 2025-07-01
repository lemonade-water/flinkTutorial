package com.flink.demo.chapter03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UDFDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1,2,3,4,5,6,7,8,9,10)
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer integer) throws Exception {
                        return integer * 2;
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务的生命周期开始");
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务的生命周期结束");
                    }
                }).print();
        env.execute();
    }
}
