# Building a real-time word count application using Apache Flink and Redpanda

Get up and running with a real-time word count application by integrating Apache Flink® with Redpanda.

Follow along with [this tutorial on the Redpanda blog](https://redpanda.com/blog/apache-flink-redpanda-real-time-word-count-application) to put this demo into action. 

---------------

Basic set up of Redpanda, Flink, and an example Java application to demonstrate stream processing between the two.

First run Maven to create a quick skeleton of an Apache Flink job.
```
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java      \
      -DarchetypeVersion=1.14.4
```

This repo contains all the files from the built Maven project output, but I only modified `pom.xml` and `StreamingJob.java` to get this application to work in Flink.

Once you've modified the files, you can build the JAR file using: `mvn clean package`

The resulting JAR can be uploaded to Flink for processing data streams. It should provide a good starting point to do more complex stream processing.

-----------------

## About Redpanda 

Redpanda is Apache Kafka® API-compatible. Any client that works with Kafka will work with Redpanda, but we have tested the ones listed [here](https://docs.redpanda.com/docs/reference/faq/#what-clients-do-you-recommend-to-use-with-redpanda).

* You can find our main project repo here: [Redpanda](https://github.com/redpanda-data/redpanda)
* Join the [Redpanda Community on Slack](https://redpanda.com/slack)
* [Sign up for Redpanda University](https://university.redpanda.com/) for free courses on data streaming and working with Redpanda
