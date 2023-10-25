package wind;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.stream.Stream;

public class WindTurbineProducer {
    public static void main(final String[] args) throws IOException {
        final Properties props = new Properties();
        String configFile = "producer.properties";
        if (args.length == 1) {
            configFile = args[0];
        }
        props.load(new FileReader(configFile));
        // How to serialize Keys?
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if(props.getOrDefault("enable.schemas", "true") != "true") {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WindTurbineDataSerializer.class);
        } else {
            // How to serialize Values?
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
            // Schema Registry/Karapace URL
            props.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");
            // Do not register Schemas automatically
            props.put(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
            // Fail on invalid schemas
            props.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, true);
            // Define which schema ID to use
            props.put(KafkaJsonSchemaSerializerConfig.USE_SCHEMA_ID, 1);
        }

        final String TOPIC = props.getProperty("topic");
        double msgsPerSec = Double.parseDouble(props.getProperty("producer.msgs.per.sec", "1"));
        boolean logInfos = props.getProperty("app.log.infos", "true").equals("true");

        // The WindTurbineDataSupplier creates a Stream of approximately `msgsPerSec` messages per seconds for you to produce
        final Stream<WindTurbineData> windTurbineDataStream = Stream.generate(new WindTurbineDataSupplier(50, msgsPerSec));

        Stats stats = new Stats(5000);

        // initialize a producer
        // Please always close the producers. try(var) {} closes it automatically
        try (Producer<String, WindTurbineData> producer = new KafkaProducer<>(props)) {
            windTurbineDataStream.forEach(turbineData -> {
                String key = turbineData.windTurbineId;

                long sendStartMs = System.currentTimeMillis();

                ProducerRecord<String, WindTurbineData> producerRecord = new ProducerRecord<>(TOPIC, key, turbineData);

                Callback cb = stats.nextCompletion(sendStartMs, stats);
                producer.send(producerRecord, cb);
                if (logInfos) {
                    System.out.println("Produced data for wind turbine " + turbineData.windTurbineId);
                }
            });
        } finally {
            stats.printTotal();
        }
    }
}
