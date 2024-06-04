package youtubedatacleaning;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class YouTubeDataCleaningJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set the desired restart strategy
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        10, // Specify the number of restart attempts
                        Time.seconds(10) // Specify the delay between restart attempts
                )
        );
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Set up the Kafka properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "youtubeConsumer");
        kafkaProps.setProperty("enable.auto.commit", "true");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "youtube",
                new SimpleStringSchema(),
                kafkaProps
        );

        // Read data from Kafka
        DataStream<String> input = env.addSource(kafkaConsumer);

        // Perform data cleaning operations
        DataStream<String> cleanedData = input.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                // Parse the JSON object
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(value);

                // Extract the desired fields from the JSON object
                String videoId = jsonNode.get("video_id").asText();
                String channelTitle = jsonNode.get("channelTitle").asText();
                String title = jsonNode.get("title").asText();
                String description = jsonNode.get("description").asText();
				String simulationTime = jsonNode.get("simulation_time").asText();
                
				// Create a new JSON object with the cleaned data
                ObjectNode cleanedJson = JsonNodeFactory.instance.objectNode()
                        .put("platform_id", 1)
                        .put("post_id", videoId)
                        .put("username", channelTitle)
                        .put("title", title)
                        .put("content", description)
                        .put("time", simulationTime);

                // Serialize the cleaned JSON object back to a string
                return cleanedJson.toString();
            }
        });

        // Set up the Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");

        // Create a Kafka producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "rawdata",
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        // Write cleaned data to Kafka
        cleanedData.addSink(kafkaProducer);

        // Execute the Flink job
        env.execute("YouTube Data Cleaning Job");
    }
}