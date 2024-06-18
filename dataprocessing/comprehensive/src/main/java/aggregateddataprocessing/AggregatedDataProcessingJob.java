package aggregateddataprocessing;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.PreparedStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class AggregatedDataProcessingJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set the desired restart strategy
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        10, // Specify the number of restart attempts
                        org.apache.flink.api.common.time.Time.seconds(10) // Specify the delay between restart attempts
                )
        );
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Set up the Kafka properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "contentConsumer");
        kafkaProps.setProperty("enable.auto.commit", "true");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "contentword",
                new SimpleStringSchema(),
                kafkaProps
        );

        DataStream<String> dataStream = env.addSource(kafkaConsumer);

        DataStream<Tuple3<Long, String, Long>> result = dataStream
            .flatMap(new FlatMapFunction<String, Tuple3<Long, String, Long>>() {
                @Override
                public void flatMap(String value, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                    // Parse the JSON object
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode = mapper.readTree(value);

                    // Extract the desired fields from the JSON object
                    String content_word = jsonNode.get("content_word").asText();
                    Long count = jsonNode.get("count").asLong();
                    String time = jsonNode.get("time").asText();
                    String modifiedTime = time.substring(0, time.length() - 3) + ":00";

                    // Parse the time string to LocalTime
                    LocalTime localTime = LocalTime.parse(modifiedTime, DateTimeFormatter.ofPattern("HH:mm:ss"));

                    // Set the date to the current date
                    LocalDateTime dateTimeWithCurrentDate = LocalDateTime.of(LocalDate.now(), localTime);
                    Instant instantWithCurrentDate = dateTimeWithCurrentDate.atZone(ZoneId.systemDefault()).toInstant();

                    if(content_word != null && !content_word.trim().isEmpty())
                        out.collect(new Tuple3<>(instantWithCurrentDate.toEpochMilli(), content_word, count));
                }
            })
            .keyBy(value -> value.f1)
            .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
            .sum(2);

        CassandraSink.addSink(result)
                .setQuery("INSERT INTO comprehensive.WordCount(time, word, count) values (?, ?, ?);")
                .setHost("127.0.0.1")
                .build();

        DataStream<Tuple3<Long, String, Long>> result2 = dataStream
            .flatMap(new FlatMapFunction<String, Tuple3<Long, String, Long>>() {
                @Override
                public void flatMap(String value, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                    // Parse the JSON object
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode = mapper.readTree(value);

                    // Extract the desired fields from the JSON object
                    String content_word = jsonNode.get("platform_id").asText() + jsonNode.get("content_word").asText();
                    Long count = jsonNode.get("count").asLong();
                    String time = jsonNode.get("time").asText();
                    String modifiedTime = time.substring(0, time.length() - 3) + ":00";

                    // Parse the time string to LocalTime
                    LocalTime localTime = LocalTime.parse(modifiedTime, DateTimeFormatter.ofPattern("HH:mm:ss"));

                    // Set the date to the current date
                    LocalDateTime dateTimeWithCurrentDate = LocalDateTime.of(LocalDate.now(), localTime);
                    Instant instantWithCurrentDate = dateTimeWithCurrentDate.atZone(ZoneId.systemDefault()).toInstant();

                    if(content_word != null && !content_word.trim().isEmpty())
                        out.collect(new Tuple3<>(instantWithCurrentDate.toEpochMilli(), content_word, count));
                    
                }
            })
            .keyBy(value -> value.f1)
            .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
            .sum(2);

        DataStream<Tuple4<Long, Long, String, Long>> outputStream = result2.map(
            new MapFunction<Tuple3<Long, String, Long>, Tuple4<Long, Long, String, Long>>() {
                @Override
                public Tuple4<Long, Long, String, Long> map(Tuple3<Long, String, Long> value) {
                    String inputString = value.f1;
                    Long platform_id = inputString.isEmpty() ? 0L : (long)inputString.charAt(0) - 48;
                    String content_word = inputString.length() > 1 ? inputString.substring(1) : "";
                    return new Tuple4<>(value.f0, platform_id, content_word, value.f2);
                }
            }
        );

        CassandraSink.addSink(outputStream)
                .setQuery("INSERT INTO comprehensive.PlatformWordCount(time, platform_id, word, count) values (?, ?, ?, ?);")
                .setHost("127.0.0.1")
                .build();

        env.execute("Aggregated Data Processing Job");
    }
}