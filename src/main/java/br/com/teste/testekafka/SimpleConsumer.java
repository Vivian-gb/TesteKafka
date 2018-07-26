package br.com.teste.testekafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer {
    public static void main(final String[] args) throws Exception {
        String topicName = "poc-kafka";
        // Check arguments length value
        if (args.length > 0) {
            // Assign topicName to string variable
            topicName = args[0];
        }
        Properties props = new Properties();

        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("group.id", "tst");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("enable.auto.commit", "true");
        // props.put("client.id", "MyConsumer");
        // props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                        "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                        "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));

        // print the topic name
        System.out.println("Subscribed to topic " + topicName);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    // print the offset,key and value for the consumer records.
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                                    record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}