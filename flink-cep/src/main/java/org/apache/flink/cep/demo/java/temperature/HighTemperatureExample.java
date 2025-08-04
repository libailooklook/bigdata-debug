package org.apache.flink.cep.demo.java.temperature;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class HighTemperatureExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 模拟数据流，这里从元素中读取
        DataStream<Event> input = env.fromElements(
                new Event("sensor1", 45.0, System.currentTimeMillis()),
                new Event("sensor1", 55.0, System.currentTimeMillis()+1000), // 第一个高温事件
                new Event("sensor1", 51.0, System.currentTimeMillis()+2000), // 第二个高温事件，在10秒内
                new Event("sensor2", 52.0, System.currentTimeMillis()+3000),
                new Event("sensor2", 53.0, System.currentTimeMillis()+4000) // 另一个传感器的连续两个高温事件
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

        // 根据传感器ID分组
        KeyedStream<Event, String> keyedStream = input.keyBy(Event::getId);

        // 定义模式：连续两个高温事件（>=50），在10秒内
        Pattern<Event, ?> pattern = Pattern.<Event>begin("first")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getTemperature() >= 50;
                    }
                })
                .next("second")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getTemperature() >= 50;
                    }
                })
                .within(Time.seconds(10));

        // 应用模式
        PatternStream<Event> patternStream = CEP.pattern(keyedStream, pattern);

        // 处理匹配事件
        DataStream<String> alerts = patternStream.process(
                new PatternProcessFunction<Event, String>() {
                    @Override
                    public void processMatch(
                            Map<String, List<Event>> match,
                            Context ctx,
                            Collector<String> out) throws Exception {
                        Event first = match.get("first").get(0);
                        Event second = match.get("second").get(0);
                        out.collect("传感器 " + first.getId() + " 连续高温: " +
                                first.getTemperature() + " 然后 " + second.getTemperature());
                    }
                });

        alerts.print("高温告警");

        env.execute("Flink CEP Example");
    }

    public static class Event {
        private String id;
        private double temperature;
        private long timestamp;

        public Event() {

        }

        public Event(String id, double temperature, long timestamp) {
            this.id = id;
            this.temperature = temperature;
            this.timestamp = timestamp;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public double getTemperature() {
            return temperature;
        }

        public void setTemperature(double temperature) {
            this.temperature = temperature;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "id='" + id + '\'' +
                    ", temperature=" + temperature +
                    '}';
        }
    }

}
