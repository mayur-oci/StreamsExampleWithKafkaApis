package kafka.sdk.oss.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

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

    public static void consume(String topic, String cgName) {
        streamOrKafkaTopicName = topic;
        final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(getKafkaProperties(cgName));

        consumer.subscribe(Collections.singletonList(streamOrKafkaTopicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.printf("onPartitionsRevoked - consumerGroupName: %s, partitions: %s%n", cgName,
                        formatPartitions(partitions));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.printf("onPartitionsAssigned - consumerName: %s, partitions: %s%n", cgName,
                        formatPartitions(partitions));
            }
        });

        System.out.println("\n\n\nStarted Consumer for OSS with consumer group name: " + cgName);

        while (true) {
//            System.out.println("Partitions assigned are :");
//            Set<TopicPartition> partitions = consumer.assignment();
//            partitions.forEach(part -> System.out.println(part.partition()));

            System.out.println("\n\nPolling records: ");
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(30000l));

            System.out.println("size of records polled is " + records.count());
            for (ConsumerRecord<Integer, String> record : records) {
                 System.out.println("Received message: (key-" + record.key() + ", Value- " + record.value() + ") at offset " + record.offset() + " from partition " + record.partition());
            }

            consumer.commitSync();
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //consumer.close();
    }

    private static List<String> formatPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream().map(topicPartition ->
                String.format("topic: %s, partition: %s", topicPartition.topic(), topicPartition.partition()))
                .collect(Collectors.toList());
    }
}
