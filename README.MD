#Commands

## Compile and build
```
mvn clean install
```

## Producer one by one
```
mvn exec:java -Dexec.arguments="p,-t=<topic_name>"
```

## Producer Bulk

```
mvn exec:java -Dexec.arguments="p,-t=OssFn,-isBulk=true"
```

## Consumer 

```
mvn exec:java -Dexec.arguments="c,-t=<topic_name>,-cg=<name_of_consumer_group>"
```

## Create Topic
```
mvn compile exec:java -Dexec.arguments="admin,-t=<topic_name>,-pc=<number_of_partitions_for_the_topic>"
```