package kafka.sdk.oss.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Properties;

public class KafkaAdmin {

    static String bootstrapServers = "cell-1.streaming.us-sanjose-1.oci.oraclecloud.com:9092";
    static String tenancyName = "intrandallbarnes";
    static String username = "mayur.raleraskar@oracle.com";
    static String streamPoolId = "ocid1.streampool.oc1.us-sanjose-1.amaaaaaauwpiejqawbcccfmvdkctu5vbmhwlogzsjss4haz7nuepc4ihk3ea";
    static String authToken = "2m{s4WTCXysp:o]tGx4K";
    static String streamOrKafkaTopicName = "OssFn";

    static Properties properties = new Properties();
    static Admin admin = null;

    static {
        try {
            properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            properties.put("sasl.mechanism", "PLAIN");
            final String value = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                    + tenancyName + "/"
                    + username + "/"
                    + streamPoolId + "\" "
                    + "password=\""
                    + authToken + "\";";
            properties.put("sasl.jaas.config", value);

            admin = Admin.create(properties);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("\n\n\n");
        }
    }

    public static void createTopic(String topicName, int pc) {
        short rf = 3;
        NewTopic topic = new NewTopic(topicName, pc, rf);

        try {
            admin.createTopics(Collections.singleton(topic));
            System.out.println("Created the topic: " + topicName);

            for (TopicListing topicListing : admin.listTopics().listings().get()) {
                System.out.println(topicListing);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Topic creation failed");
        }
    }

    public static void descCluster() {
        try {
            for (Node node : admin.describeCluster().nodes().get()) {
                System.out.println("-- node: " + node.id() + " --");
                ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, "0");
                DescribeConfigsResult dcr = admin.describeConfigs(Collections.singleton(cr));
                dcr.all().get().forEach((k, c) -> {
                    c.entries()
                            .forEach(configEntry -> {
                                System.out.println(configEntry.name() + "= " + configEntry.value());
                            });
                });
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
