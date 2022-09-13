package io.csk.demo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-first-cons");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("first-topic"));

        Thread mainThread = Thread.currentThread();

        // adding shutdown hook, new Thread is last that will exit here not main thread.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Detected Shutdown, calling Wakeup Consumer...");
            consumer.wakeup();

            try {
                // join the main thread to allow the execution of code in main Thread.
                mainThread.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("Thread is exiting....");
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                records.forEach(record -> {
                    System.out.println(record.key() + "  " + record.value());
                });
            }
        }
        catch (WakeupException e) {
            System.out.println("Consumer wakeup...Exception");
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
        finally {
            consumer.close();
            System.out.println("Closed the consumer....");
        }

        System.out.println("Exiting Main Thread...");
    }
}
