package wind;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
        // How to serialize Values?
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, WindTurbineDataSerializer.class);

        final String TOPIC = props.getProperty("topic");
        double msgsPerSec = Double.parseDouble(props.getProperty("producer.msgs.per.sec", "1"));
        boolean logInfos = props.getProperty("app.log.infos", "true").equals("true");

        // The WindTurbineDataSupplier creates a Stream of approximately `msgsPerSec` messages per seconds for you to produce
        final Stream<WindTurbineData> windTurbineDataStream = Stream.generate(new WindTurbineDataSupplier(50, msgsPerSec));

        // initialize a producer
        // Please always close the producers. try(var) {} closes it automatically
        try (Producer<String, WindTurbineData> producer = new KafkaProducer<>(props)) {
            windTurbineDataStream.forEach(turbineData -> {
                String key = turbineData.windTurbineId;

                ProducerRecord<String, WindTurbineData> producerRecord = new ProducerRecord<>(TOPIC, key, turbineData);


                producer.send(producerRecord);
                if (logInfos) {
                    System.out.println("Produced data for wind turbine " + turbineData.windTurbineId);
                }
            });
        }
    }
}
