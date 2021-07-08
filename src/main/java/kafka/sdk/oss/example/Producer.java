package kafka.sdk.oss.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

public class Producer {

    static String bootstrapServers = "cell-1.streaming.us-sanjose-1.oci.oraclecloud.com:9092";
    static String tenancyName = "intrandallbarnes";
    static String username = "mayur.raleraskar@oracle.com";
    static String streamPoolId = "ocid1.streampool.oc1.us-sanjose-1.amaaaaaauwpiejqawbcccfmvdkctu5vbmhwlogzsjss4haz7nuepc4ihk3ea";
    static String authToken = "2m{s4WTCXysp:o]tGx4K";
    static String streamOrKafkaTopicName = "OssFn";
    static KafkaProducer producer = null;
    static Properties properties = new Properties();


    private static void producerInitialize() {
        try {
            Properties properties = getKafkaProperties();
            producer = new KafkaProducer<>(properties);
            System.out.println("\n\n\nStarted Producer for OSS");
        } catch (Exception e) {
            System.err.println("Error: exception " + e);
        }
    }

    private static Properties getKafkaProperties() {
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

    public static void produce(String topic) {
        streamOrKafkaTopicName = topic;
        producerInitialize();


        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            String input = "";
            while (true) {

                System.out.print("Enter message key(OrderId) and value seperated by '-' (q to quite): ");
                input = br.readLine();

                if ("q".equalsIgnoreCase(input))
                    break;

                sendMessage(input.split("-")[0], input.split("-")[1]);
                // producer.send() is async, to make sure all messages are sent we use producer.flush()
                producer.flush();
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
                //System.err.println("TRACE: Sent msg to partition-" + md.partition() + " with offset " + md.offset() + " at " + md.timestamp());
            }
        });
    }

    public static void sendBulk(String topic) {
        streamOrKafkaTopicName = topic;

        producerInitialize();
        String[] statesArray = {"Order Received from customer", "Order Accepted By Restaurant", "Order Accepted By Delivery Boy", "Food Prepared and ready for pickup", "Food on delivery", "Food delievered"};
        List<String> orderStates = Arrays.asList(statesArray);

        Integer MAX_ORDERID = 1000;

        Integer[] orderIdArray = new Integer[MAX_ORDERID / 10];
        for (Integer i = 10, j = 0; i <= MAX_ORDERID; i = i + 10, j++) {
            orderIdArray[j] = i;
        }


        for (String orderState : orderStates) {
            Set<Integer> orderDoneSet = new HashSet<Integer>();

            while (orderDoneSet.size() < (MAX_ORDERID / 10)) {
                Integer orderIdRandomlyPicked = null;
                do {
                    orderIdRandomlyPicked = getRandom(orderIdArray);
                } while (orderDoneSet.contains(orderIdRandomlyPicked));

                orderDoneSet.add(orderIdRandomlyPicked);

                System.out.println("sending message to topic-" + topic +
                        " for orderId-" + orderIdRandomlyPicked + " for state-" + orderState);
                sendMessage("OrderId-" + orderIdRandomlyPicked.toString(), orderState);
            }
            producer.flush();
            System.out.println("State Produced " + orderState + " for all orders!");
        }

        producer.close();
    }

    static Integer getRandom(Integer[] array) {
        int rnd = new Random().nextInt(array.length);
        return array[rnd];
    }
}