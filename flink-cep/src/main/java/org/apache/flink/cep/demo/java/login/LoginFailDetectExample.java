package org.apache.flink.cep.demo.java.login;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;
import java.util.List;
import java.util.Map;
public class LoginFailDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1. 获取登录事件流，并提取时间戳、生成水位线
        KeyedStream<Event, String> stream = env
                .fromElements(
                        new Event("user_1", "192.168.0.1", "fail", 2000L),
                        new Event("user_1", "192.168.0.2", "fail", 3000L),
                        new Event("user_2", "192.168.1.29", "fail", 4000L),
                        new Event("user_1", "171.56.23.10", "fail", 5000L),
                        new Event("user_2", "192.168.1.29", "fail", 7000L),
                        new Event("user_2", "192.168.1.29", "fail", 8000L),
                        new Event("user_2", "192.168.1.29", "success", 6000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<Event>() {
                                            @Override
                                            public long extractTimestamp(Event event, long l) {
                                                return event.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(r -> r.userId);
        // 2. 定义Pattern，连续的三个登录失败事件
        Pattern<Event, Event> pattern = Pattern.<Event>begin("first")    // 以第一个登录失败事件开始
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("second")    // 接着是第二个登录失败事件
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                })
                .next("third")     // 接着是第三个登录失败事件
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event loginEvent) throws Exception {
                        return loginEvent.eventType.equals("fail");
                    }
                });
        // 3. 将Pattern应用到流上，检测匹配的复杂事件，得到一个PatternStream
        PatternStream<Event> patternStream = CEP.pattern(stream, pattern);
        // 4. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
        patternStream
                .select(new PatternSelectFunction<Event, String>() {
                    @Override
                    public String select(Map<String, List<Event>> map) throws Exception {
                        Event first = map.get("first").get(0);
                        Event second = map.get("second").get(0);
                        Event third = map.get("third").get(0);
                        return first.userId + " 连续三次登录失败！登录时间：" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp;
                    }
                })
                .print("连续登录失败");
        env.execute();
    }

    public static class Event {
        public String userId;
        public String ipAddress;
        public String eventType;
        public Long timestamp;
        public Event(String userId, String ipAddress, String eventType, Long timestamp) {
            this.userId = userId;
            this.ipAddress = ipAddress;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        public Event() {}

        @Override
        public String toString() {
            return "LoginEvent{" +
                    "userId='" + userId + '\'' +
                    ", ipAddress='" + ipAddress + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}