package io.csk.demo.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private final static String bootstrapServers = "localhost:9092";
    private final static String topic = "wikimedia-recentchange";
    private final static String wikiStreamUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) throws InterruptedException {

        System.out.println("Wikimedia Producer......");
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(eventHandler, URI.create(wikiStreamUrl));
        EventSource eventSource = eventSourceBuilder.build();

        // start event sourcing, this will start in new thread. So we need to stop main thread from exit.
        eventSource.start();

        // Pausing main thread
        TimeUnit.MINUTES.sleep(10);
    }
}
