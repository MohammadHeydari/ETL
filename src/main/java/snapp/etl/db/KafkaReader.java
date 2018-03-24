package snapp.etl.db;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import snapp.etl.MessageReceiver;
import snapp.etl.util.Configuration;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class KafkaReader {

    public void subscribe() {
        Properties props = new Properties();
        Configuration configuration = Configuration.getInstance();
        ElasticSearch client = ElasticSearch.connect();
        String engineOption = configuration.getConfig("engine.to.start");
        Thread eventTopicReader = new Thread() {
            @Override
            public void run() {
                ExecutorService eventThreadPool = Executors.newFixedThreadPool(Integer.parseInt(configuration.getConfig("event.injector.thread.size")));
                props.put("bootstrap.servers", configuration.getConfig("kafka.bootstrap.servers"));
                props.put("group.id", configuration.getConfig("kafka.event.group.id"));
                props.put("key.deserializer", StringDeserializer.class.getName());
                props.put("value.deserializer", StringDeserializer.class.getName());
                props.put("enable.auto.commit", "true");
                props.put("auto.commit.interval.ms", "1000");
                KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
                consumer.subscribe(Arrays.asList(configuration.getConfig("kafka.event.topic.name")));

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    Iterator<ConsumerRecord<String, String>> messages = records.iterator();
                    if (messages.hasNext()) {
                        eventThreadPool.execute(new MessageReceiver(client, "event", messages));
                    }
                }
            }
        };
        Thread locationTopicReader = new Thread() {
            @Override
            public void run() {
                ExecutorService locationThreadPool = Executors.newFixedThreadPool(Integer.parseInt(configuration.getConfig("location.injector.thread.size")));
                props.put("bootstrap.servers", configuration.getConfig("kafka.bootstrap.servers"));
                props.put("group.id", configuration.getConfig("kafka.location.group.id"));
                props.put("key.deserializer", StringDeserializer.class.getName());
                props.put("value.deserializer", StringDeserializer.class.getName());
                props.put("enable.auto.commit", "true");
                props.put("auto.commit.interval.ms", "1000");
                KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
                consumer.subscribe(Arrays.asList(configuration.getConfig("kafka.location.topic.name")));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    Iterator<ConsumerRecord<String, String>> messages = records.iterator();
                    if (messages.hasNext()) {
                        locationThreadPool.execute(new MessageReceiver(client, "location", messages));
                    }
                }
            }
        };
        switch (engineOption) {
            case "ALL": {
                eventTopicReader.start();
                System.out.println("Event Topic Reader Started...");
                locationTopicReader.start();
                System.out.println("Location Topic Reader Started...");
                break;
            }
            case "EVENT": {
                eventTopicReader.start();
                break;
            }
            case "LOCATION": {
                locationTopicReader.start();
                break;
            }
            default: {
                System.out.println("No engine selected");
                break;
            }
        }
    }
}
