package kafka.sdk.oss.example;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(name = "OssWithKafkaApi", mixinStandardHelpOptions = true, version = "1.0",
        description = "Example showing Oracle Streaming Service compatibility with Kafka APIs")
class Starter implements Callable<Integer> {

    @Parameters(index = "0", description = "Producer or Consumer?")
    private String pOrC = null;

    @Option(names = {"-cg", "--consumer-group"}, description = "name of consumer group in case when running as consumer")
    private String cgName = "";

    // this example implements Callable, so parsing, error handling and handling user
    // requests for usage help or version help can be done with one line of code.
    public static void main(String... args) {
        System.out.println("\n\n\n");
        int exitCode = new CommandLine(new Starter()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception { // your business logic goes here...
        if (!"p".equals(pOrC) && !"c".equals(pOrC)) {
            System.out.println("plz specify p for producer and c for consumer");
            return 1;
        }
        if (pOrC.equals("c") && cgName.isEmpty()) {
            System.out.println("For Consumer mode, plz specify consumer-group name");
            return 1;
        }

        if (pOrC.equals("p"))
            Producer.produce();
        else
            Consumer.consume(cgName);

        return 0;
    }
}
