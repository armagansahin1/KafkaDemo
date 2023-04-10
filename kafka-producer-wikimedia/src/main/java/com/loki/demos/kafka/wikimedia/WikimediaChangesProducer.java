package com.loki.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",bootstrapServer);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // set high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        // Create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer,topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // start producer producer thread
        eventSource.start();

        //we produce fot 10 min and block the program until then
        TimeUnit.MINUTES.sleep(10);


    }
}
