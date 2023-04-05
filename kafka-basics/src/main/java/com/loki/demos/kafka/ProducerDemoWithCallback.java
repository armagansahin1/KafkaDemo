package com.loki.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello");

        //Create Producer Properties

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username='3eIJeMKodslNeSuZtEUX5t' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzZUlKZU1Lb2RzbE5lU3VadEVVWDV0Iiwib3JnYW5pemF0aW9uSWQiOjcyMDYxLCJ1c2VySWQiOjgzNjM5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI5MDIwMTVlNi1jMDBlLTQ3NjQtODA5OS0wMTBhYjQ3NzY1Y2UifX0.P30KTBWpJ7DV7SrVYgk6DEW9vTUPfqvybp26VqlZzDA';");
        properties.setProperty("sasl.mechanism","PLAIN");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0 ; i < 10 ; i++){
            // Create a Producer Record
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","hello world " + i);

            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null){
                        log.info("Recieved new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing" , e);
                    }

                }
            });
        }



        //flush and close the producer

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        producer.close();


    }
}
