package io.csk.demo.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private final Logger LOG = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    private final Producer<String, String> producer;
    private final String topic;

    public WikimediaChangeHandler(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }


    @Override
    public void onOpen() throws Exception {
        // nothing
    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        LOG.info(messageEvent.getData());

        // publishing message to Kafka
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {
        // nothing
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.error("Error streaming wiki data ", throwable.getMessage());
    }
}
