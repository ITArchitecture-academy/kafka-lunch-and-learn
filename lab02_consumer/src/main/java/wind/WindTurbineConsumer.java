package wind;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class WindTurbineConsumer {
    public static void main(final String[] args) throws IOException, InterruptedException {
        final Properties props = new Properties();
        String configFile = "consumer.properties";
        if (args.length == 1) {
            configFile = args[0];
        }
        props.load(new FileReader(configFile));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
        // Schema Registry/Karapace URL
        props.put(KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
        // Do not register Schemas automatically
        props.put(KafkaJsonSchemaDeserializerConfig.AUTO_REGISTER_SCHEMAS, false);
        // Fail on invalid schemas
        props.put(KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA, true);
        // Define which schema ID to use
        props.put(KafkaJsonSchemaDeserializerConfig.USE_SCHEMA_ID, 1);
        // Define which class should be the deserialization target
        props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, WindTurbineData.class);
        props.put("json.oneof.for.nullables", false);


        final String TOPIC = props.getProperty("topic");
        long processingTimeMs = Long.parseLong(props.getProperty("processing.time.ms", "1"));
        boolean logInfos = props.getProperty("app.log.infos", "true").equals("true");


        final Consumer<String, WindTurbineData> consumer = new KafkaConsumer<>(props);

        try (consumer) {
            consumer.subscribe(List.of(TOPIC));

            System.out.println("Started…");
            while (true) {
                ConsumerRecords<String, WindTurbineData> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, WindTurbineData> record : records) {
                    String key = record.key();
                    WindTurbineData value = record.value();
                    // "Processing" Message
                    Thread.sleep(processingTimeMs);
                    if (logInfos) {
                        System.out.println(key + ": " + value);
                    }
                }
            }
        }
    }
}
