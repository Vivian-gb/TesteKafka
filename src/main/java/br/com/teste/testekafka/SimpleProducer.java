package br.com.teste.testekafka;

//import util.properties packages
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//Create java class named "SimpleProducer"
public class SimpleProducer {

    public static void main(final String[] args) throws Exception {
        String topicName = "poc-kafka";
        // Check arguments length value
        if (args.length > 0) {
            // Assign topicName to string variable
            topicName = args[0];
        }

        // create instance for properties to access producer configs
        Properties props = new Properties();

        // Assign localhost id

        props.put("bootstrap.servers", "127.0.0.1:9092");

        // Set acknowledgements for producer requests.
        props.put("acks", "all");

        // If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        // Specify buffer size in config
        props.put("batch.size", 16384);

        // Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        // The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,
                            Integer.toString(i), Integer.toString(i));
            producer.send(rec, new Callback() {
                @Override
                public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                    if (metadata != null) {
                        System.out.println("Message sent to topic ->" + metadata.topic() + " stored at offset->"
                                        + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        producer.close();
    }
}