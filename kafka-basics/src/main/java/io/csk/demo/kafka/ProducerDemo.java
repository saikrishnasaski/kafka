package io.csk.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Producer<String, String> producer = new KafkaProducer<String, String>(props);



        for (int i=0; i<10; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first-topic", "csk", "Apache Kafka Java Producerr in loop....." + i);
            // send is asynchronous. So program may exit before publishing the record.
            producer.send(producerRecord, ((recordMetadata, e) -> {
                if (e != null) {
                    System.out.println("Error occured while publishing the message");
                    e.printStackTrace();
                }
                else {
                    System.out.println(producerRecord.key());
                    System.out.println(producerRecord.value());
                    System.out.println(recordMetadata.topic());
                    System.out.println(recordMetadata.partition());
                    System.out.println(recordMetadata.offset());
                    System.out.println(recordMetadata.timestamp());
                }
            }));

            Thread.sleep(1000);
        }

        // flush is synchronous
        producer.flush();

        // close producer
        producer.close();

    }
}
