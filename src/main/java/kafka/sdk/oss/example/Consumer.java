package kafka.sdk.oss.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    static String bootstrapServers = "cell-1.streaming.us-sanjose-1.oci.oraclecloud.com:9092";
    static String tenancyName = "intrandallbarnes";
    static String username = "mayur.raleraskar@oracle.com";
    static String streamPoolId = "ocid1.streampool.oc1.us-sanjose-1.amaaaaaauwpiejqawbcccfmvdkctu5vbmhwlogzsjss4haz7nuepc4ihk3ea";
    static String authToken = "2m{s4WTCXysp:o]tGx4K";
    static String streamOrKafkaTopicName = "OssFn";

    private static Properties getKafkaProperties(String consumerGroupName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", consumerGroupName);
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("auto.offset.reset", "earliest");
        final String value = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + tenancyName + "/"
                + username + "/"
                + streamPoolId + "\" "
                + "password=\""
                + authToken + "\";";
        props.put("sasl.jaas.config", value);
        return props;
    }

    public static void consume(String cgName) {
        final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(getKafkaProperties(cgName));

        consumer.subscribe(Collections.singletonList(streamOrKafkaTopicName));

        System.out.println("\n\n\nStarted Consumer for OSS with consumer group name: " + cgName);

        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(30000l));

        System.out.println("size of records polled is " + records.count());
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("Received message: (key/OrderId-" + record.key() + ", Value- " + record.value() + ") at offset " + record.offset() + " from partition " + record.partition());
        }

        consumer.commitSync();
        consumer.close();
    }
}
