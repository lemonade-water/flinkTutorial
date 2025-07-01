package com.flink.demo.chapter12;

import com.flink.demo.chapter05.WaterSensorMapFunction;
import com.flink.demo.pojo.WaterSensor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 阈值触发报警，阈值是动态的
 */
public class OperationBroadcastExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorMapFunction());
        // 配置流（用来广播配置）
        DataStreamSource<String> configDS = env.socketTextStream("hadoop102", 8888);

        // TODO 1. 将 配置流 广播
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = configDS.broadcast(broadcastMapState);

        // TODO 2.把 数据流 和 广播后的配置流 connect
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDS.connect(configBS);

        // TODO 3.调用 process
        sensorBCS.process(new BroadcastProcessFunction<WaterSensor, String, String>() {
                    /**
                     * 数据流的处理方法： 数据流 只能 读取 广播状
                     态，不能修改
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processElement(WaterSensor value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // TODO 5.通过上下文获取广播状态，取出里面的值（只读，不能修改）
                        ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                        Integer threshold = broadcastState.get("threshold");
                        env.execute();
                    }
                    /**
                     * 广播流的处理方法： 广播流 既可以读取，也可以修改
                     * @param value
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        // TODO 4.通过上下文获取广播状态，修改里面的值
                        BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                        broadcastState.put("threshold", 20);
                    }
                });
        env.execute();
    }
}
