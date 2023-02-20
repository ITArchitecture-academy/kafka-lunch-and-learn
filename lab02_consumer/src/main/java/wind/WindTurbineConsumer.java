package wind;

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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WindTurbineDataDeserializer.class);
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
