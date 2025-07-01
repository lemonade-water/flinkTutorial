package com.flink.demo.chapter09;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowReduceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("hadoop102", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyBy = source.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return value!=null;
            }
        }).keyBy(WaterSensor::getId);
        //滚动窗口
        SingleOutputStreamOperator<WaterSensor> reduce = keyBy.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor w1, WaterSensor w2) {
                        System.out.println("调用reduce:" + w1 + "," + w2);
                        return new WaterSensor(w2.getId(), w2.getTs(), w1.getVc() + w2.getVc());
                    }
                });
        reduce.print();
        env.execute();
    }
}
