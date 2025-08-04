package org.apache.flink.cep.demo.java.logging;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class LoggingExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 模拟输入数据源 (实际应用可替换为Kafka/Socket等)
        DataStream<Event> input = env.fromElements(
                new Event(42, "normal", System.currentTimeMillis()),
                new Event(42, "error", System.currentTimeMillis() + 1000),
                new Event(42, "critical", System.currentTimeMillis() + 2000),
                new Event(42, "error", System.currentTimeMillis() + 3000),
                new Event(42, "normal", System.currentTimeMillis() + 4000)  // 此事件不会触发警报
        ).assignTimestampsAndWatermarks(
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
                .keyBy(r -> r.id);

        // 3. 按键分区（按事件ID分组）
        DataStream<Event> partitionedInput = input.keyBy(new KeySelector<Event, Integer>() {
            @Override
            public Integer getKey(Event value) throws Exception {
                return value.getId();
            }
        });

        // 4. 定义CEP模式：10秒内连续出现error后紧跟critical事件
        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
                .next("middle")  // 严格连续
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) {
                        return "error".equals(value.getName());
                    }
                })
                .followedBy("end")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) {
                        return "critical".equals(value.getName());
                    }
                })
                .within(Time.seconds(10));  // 时间窗口限制

        // 5. 将模式应用到输入流
        PatternStream<Event> patternStream = CEP.pattern(partitionedInput, pattern);

        // 6. 生成警报
        DataStream<Alert> alerts = patternStream.select(new PatternSelectFunction<Event, Alert>() {
            @Override
            public Alert select(Map<String, List<Event>> pattern) throws Exception {
                // 提取匹配事件
                Event start = pattern.get("start").get(0);
                Event middle = pattern.get("middle").get(0);
                Event end = pattern.get("end").get(0);

                return new Alert(
                        start.getId(),
                        "Start: " + start.getName() +
                                " → Middle: " + middle.getName() +
                                " → End: " + end.getName()
                );
            }
        });

        // 7. 输出结果（实际应用可写入Kafka/DB等）
        alerts.print("异常告警");

        // 8. 执行任务
        env.execute("CEP Pattern Detection Example");
    }

    // 定义事件类
    public static class Event {
        private int id;
        private String name;
        private long timestamp;

        public Event() {

        }

        public Event(int id, String name, long timestamp) {
            this.id = id;
            this.name = name;
            this.timestamp = timestamp;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }

    // 定义警报类
    public static class Alert implements Serializable {
        private int matchedId;
        private String patternSequence;

        public Alert() {

        }

        public Alert(int matchedId, String patternSequence) {
            this.matchedId = matchedId;
            this.patternSequence = patternSequence;
        }

        public void setMatchedId(int matchedId) {
            this.matchedId = matchedId;
        }

        public int getMatchedId() {
            return matchedId;
        }

        public void setPatternSequence(String patternSequence) {
            this.patternSequence = patternSequence;
        }

        public String getPatternSequence() {
            return patternSequence;
        }

        @Override
        public String toString() {
            return "Alert[ID=" + matchedId + ", Sequence=" + patternSequence + "]";
        }
    }
}