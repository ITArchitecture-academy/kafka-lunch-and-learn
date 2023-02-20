package wind;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class WindTurbineConsumer {
    public static void main(final String[] args) throws IOException {
        final Properties props = new Properties();
        // What do you need to configure here?
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, WindTurbineDataDeserializer.class);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "wind-turbine-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final String TOPIC = "wind-turbine-data";

        final Consumer<String, WindTurbineData> consumer = new KafkaConsumer<>(props);

        Map<String, WindTurbineData> measurements = new HashMap<>();
        try (WindTurbineAPI api = new WindTurbineAPI(8989, measurements)) {
            try (consumer) {
                consumer.subscribe(List.of(TOPIC));

                System.out.println("Started…");
                while (true) {
                    ConsumerRecords<String, WindTurbineData> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, WindTurbineData> record : records) {
                        String key = record.key();
                        WindTurbineData value = record.value();
                        measurements.put(key, value);
                        System.out.println(key + ": " + value);
                    }
                }
            }
        }
    }
}
