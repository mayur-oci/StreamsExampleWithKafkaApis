package kafka.sdk.oss.example;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(name = "OssWithKafkaApi", mixinStandardHelpOptions = true, version = "1.0",
        description = "Example showing Oracle Streaming Service compatibility with Kafka APIs")
class Starter implements Callable<Integer> {

    @Parameters(index = "0", description = "Producer or Consumer or admin apis")
    private String pOrC = null;

    @Option(names = {"-cg", "--consumer-group"}, description = "name of consumer group in case when running as consumer")
    private String cgName = "";

    @Option(names = {"-t", "--topic-name"}, description = "name of topic to be created")
    private String topicName = "";

    @Option(names = {"-pc", "--partitionCount"}, description = "number of partitions")
    private int partitionCount = 0;

    @Option(names = {"-desc", "--describe-cluster"}, description = "name of topic to be created")
    private boolean desc = false;

    @Option(names = {"-isBulk"}, description = "bulk producer? say true or false")
    private boolean isBulk = false;

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        System.out.println("\n\n\n");
        int exitCode = new CommandLine(new Starter()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception { // your business logic goes here...
        if (!"p".equals(pOrC) && !"c".equals(pOrC) && !"admin".equals(pOrC)) {
            System.out.println("plz specify p for producer, c for consumer, admin for topic creation");
            return 1;
        }
        if (topicName.isEmpty()) {
            System.out.println("topic name cant be empty");
            return 1;
        }

        if (pOrC.equals("c") && cgName.isEmpty()) {
            System.out.println("For Consumer mode, plz specify consumer-group name");
            return 1;
        }

        if (pOrC.equals("admin") && (topicName.isEmpty() || partitionCount <= 0) && desc == false) {
            System.out.println("For Admin mode, plz specify topic name and partitionCount>0");
            return 1;
        }

        if (pOrC.equals("p")) {
            if (isBulk == false)
                Producer.produce(topicName);
            else
                Producer.sendBulk(topicName);
        } else if (pOrC.equals("c"))
            Consumer.consume(topicName, cgName);
        else if (desc == false)
            KafkaAdmin.createTopic(topicName, partitionCount);
        else
            KafkaAdmin.descCluster();

        return 0;
    }
}
