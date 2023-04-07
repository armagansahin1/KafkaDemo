package com.loki.demos.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";
        //Create Consumer Properties

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='3eIJeMKodslNeSuZtEUX5t' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzZUlKZU1Lb2RzbE5lU3VadEVVWDV0Iiwib3JnYW5pemF0aW9uSWQiOjcyMDYxLCJ1c2VySWQiOjgzNjM5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI5MDIwMTVlNi1jMDBlLTQ3NjQtODA5OS0wMTBhYjQ3NzY1Y2UifX0.P30KTBWpJ7DV7SrVYgk6DEW9vTUPfqvybp26VqlZzDA';");
        properties.setProperty("sasl.mechanism","PLAIN");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);

        properties.setProperty("auto.offset.reset","earliest");

        // Create the Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread

        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){

                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow thr execution of the code in the main thread


                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }
        });

        // subscribe a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data

        try {
            while (true){
              //  log.info("Polling");

                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String,String> record : records){
                    log.info("Key : " + record.key() + " Value : " + record.value());
                    log.info("Partition : " + record.partition() + " Offset : " + record.offset());
                }
            }

        }catch (WakeupException e){
            log.info("consumer is starting to shutdown");
        }catch (Exception e){
            log.info("Unexpected exception in the consumer",e);
        }finally {
            consumer.close();
            log.info("The consumer is now gracefully shutdown");
        }



    }
}
