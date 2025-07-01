package com.flink.demo.chapter10;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class RealTimestampEventTimeWindow {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 方便调试

        // 2. 模拟真实时间戳的数据源（假设 timestamp 是 System.currentTimeMillis()）
        DataStream<Event> events = env.fromData(
                new Event("user1", parseTime("2025-05-28 12:34:55"), "login"),
                new Event("user2", parseTime("2025-05-28 12:34:56"), "view"),
                new Event("user1", parseTime("2025-05-28 12:34:57"), "click"),
                new Event("user2", parseTime("2025-05-28 12:35:01"), "purchase"), // 属于下一个窗口
                new Event("user1", parseTime("2025-05-28 12:34:59"), "logout")   // 乱序事件
        );

        // 3. 分配水位线（允许3秒乱序）
        DataStream<Event> withTimestamps = events
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        // 4. 按用户分组并开窗（5秒滚动窗口）
        DataStream<String> result = withTimestamps
                .keyBy(event -> event.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(
                            String userId,
                            Context ctx,
                            Iterable<Event> events,
                            Collector<String> out
                    ) {
                        long windowStart = ctx.window().getStart();
                        long windowEnd = ctx.window().getEnd();
                        int count = 0;
                        StringBuilder actions = new StringBuilder();

                        for (Event event : events) {
                            count++;
                            actions.append(event.action).append(", ");
                        }

                        out.collect(String.format(
                                "窗口 [%s - %s] | 用户 %s | 事件数: %d | 动作: %s",
                                formatTime(windowStart),
                                formatTime(windowEnd),
                                userId,
                                count,
                                actions.toString()
                        ));
                    }
                });

        // 5. 打印结果
        result.print();

        // 6. 执行任务
        env.execute("Real EventTime Window Example");
    }

    // 解析字符串时间为毫秒时间戳
    private static long parseTime(String timeStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.systemDefault()); // 添加默认时区
        return Instant.from(formatter.parse(timeStr)).toEpochMilli();
    }

    // 格式化时间戳为字符串
    private static String formatTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }

    // 事件类
    public static class Event {
        public String userId;
        public long timestamp; // 真实的事件时间戳（毫秒）
        public String action;

        public Event() {
        }

        public Event(String userId, long timestamp, String action) {
            this.userId = userId;
            this.timestamp = timestamp;
            this.action = action;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }
    }
}