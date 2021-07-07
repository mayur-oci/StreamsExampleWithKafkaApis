package kafka.sdk.oss.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class Producer {

    static String bootstrapServers = "cell-1.streaming.us-sanjose-1.oci.oraclecloud.com:9092";
    static String tenancyName = "intrandallbarnes";
    static String username = "mayur.raleraskar@oracle.com";
    static String streamPoolId = "ocid1.streampool.oc1.us-sanjose-1.amaaaaaauwpiejqawbcccfmvdkctu5vbmhwlogzsjss4haz7nuepc4ihk3ea";
    static String authToken = "2m{s4WTCXysp:o]tGx4K";
    static String streamOrKafkaTopicName = "OssFn";
    static KafkaProducer producer = null;
    static {
        try {
            Properties properties = getKafkaProperties();
            producer = new KafkaProducer<>(properties);
        } catch (Exception e) {
            System.err.println("Error: exception " + e);
        }
    }

    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final String value = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + tenancyName + "/"
                + username + "/"
                + streamPoolId + "\" "
                + "password=\""
                + authToken + "\";";
        properties.put("sasl.jaas.config", value);
        properties.put("retries", 3); // retries on transient errors and load balancing disconnection
        properties.put("max.request.size", 1024 * 1024); // limit request size to 1MB
        return properties;
    }

    public static void produce() {
        System.out.println("\n\n\nStarted Producer for OSS");

        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            String input = "";
            while (true) {

                System.out.print("Enter message key(OrderId) and value seperated by '-' (q to quite): ");
                input = br.readLine();

                if ("q".equalsIgnoreCase(input))
                    break;

                sendMessage(input.split("-")[0], input.split("-")[1]);
            }
            System.out.println("Closing Kafka Producer and exiting");
            producer.close();
            System.out.println("bye bye!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendMessage(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(streamOrKafkaTopicName, key, value);
        producer.send(record, (md, ex) -> {
            if (ex != null) {
                System.err.println("exception occurred in producer for review :" + record.value()
                        + ", exception is " + ex);
                ex.printStackTrace();
            } else {
                System.err.println("Sent msg to " + md.partition() + " with offset " + md.offset() + " at " + md.timestamp());
            }
        });

        // producer.send() is async, to make sure all messages are sent we use producer.flush()
        producer.flush();
    }
}