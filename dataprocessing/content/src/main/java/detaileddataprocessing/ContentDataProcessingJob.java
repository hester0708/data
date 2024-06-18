package contentdataprocessing;

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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Map;

import java.util.Properties;

public class ContentDataProcessingJob {
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
        kafkaProps.setProperty("group.id", "rawConsumer");
        kafkaProps.setProperty("enable.auto.commit", "true");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "rawdata",
                new SimpleStringSchema(),
                kafkaProps
        );

        // Read data from Kafka
        DataStream<String> input = env.addSource(kafkaConsumer);

        // Perform data processing operations
        DataStream<String> processedData = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // Parse the JSON object
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(value);

                // Extract the desired fields from the JSON object
                String platformId = jsonNode.get("platform_id").asText();
                String postId = jsonNode.get("post_id").asText();
                String title = jsonNode.get("title").asText();
                String content = jsonNode.get("content").asText();
                String time = jsonNode.get("time").asText();
				
				// Separate username string
                String fullContent = title + " " + content;
                String[] contentSplit = fullContent.split("\\s+");
                Map<String, Integer> contentMap = new HashMap<String, Integer>();
                for(String word: contentSplit){
                    word = word.toLowerCase();
                    word = word.replaceAll("[^\\p{Alnum}\\s]", "");
                    word = word.replaceAll("null", "");
                    if(!word.equals("")) contentMap.put(word, 1);
                }

                // Create JSON objects with splitted data
                for (Map.Entry<String, Integer> entry: contentMap.entrySet()) {
                    ObjectNode wordJson = JsonNodeFactory.instance.objectNode()
                        .put("platform_id", platformId)
                        .put("post_id", postId)
                        .put("content_word", entry.getKey())
                        .put("count", entry.getValue())
                        .put("time", time);
                        
                    String wordData = wordJson.toString();
                    out.collect(wordData);
                }
            }
        });

        // Set up the Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");

        // Create a Kafka producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "contentword",
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        // Write cleaned data to Kafka
        processedData.addSink(kafkaProducer);

        // Execute the Flink job
        env.execute("Content Data Processing Job");
    }
}