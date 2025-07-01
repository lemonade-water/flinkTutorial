package com.flink.demo.chapter09;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;
import java.util.Optional;

public class AggregateWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction())
                .filter(new FilterFunction<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return value != null;
                    }
                }).keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new AggregateFunction<WaterSensor, Integer,String>(){
                    //创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("创建累加器");
                        return 0;
                    }
                    //将输入的元素添加到累加器中
                    @Override
                    public Integer add(WaterSensor waterSensor, Integer integer) {
                        System.out.println("调用add方法,value="+integer);
                        return Optional.of(waterSensor.getVc()).orElse(0)+integer;
                    }

                    //从累加器中提取聚合的输出结果
                    @Override
                    public String getResult(Integer integer) {
                        System.out.println("从累加器中提取聚合的输出结果");
                        return integer.toString();
                    }
                    //只有会话窗口会用到  合并两个累加器，并将合并后的状态作为一个累加器返回。
                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        System.out.println("调用merge方法");
                        return null;
                    }
                }).print();
        ;
        env.execute();
    }
}
