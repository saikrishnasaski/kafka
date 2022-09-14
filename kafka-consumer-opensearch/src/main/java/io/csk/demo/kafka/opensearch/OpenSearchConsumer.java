package io.csk.demo.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

public class OpenSearchConsumer {

    private final static String topic = "wikimedia-recentchange";

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws IOException {
        // create opensearch high level client
        RestHighLevelClient openSearchClient = RestHighLevelClientFactory.createOpenSearchClient();

        // create Kafka Client.
        KafkaConsumer<String, String> kafkaConsumer = KafkaConsumerFactory.createKafkaConsumer();
        kafkaConsumer.subscribe(Collections.singleton(topic));

        try (openSearchClient; kafkaConsumer) {
            GetIndexRequest getIndexRequest = new GetIndexRequest("wikimedia");
            boolean indexExists = openSearchClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                LOG.info("Wikimedia index is created.");
            }
            else {
                LOG.info("Wikimedia index already exists.");
            }

            while (true) {
                LOG.info("Polling for new records.");
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

                LOG.info("Received {} records", records.count());

                records.forEach(record -> {
                    // for idempotent consumer, we need to send uniqueId for every request.

                    // extract Id from message
                    String id = extractId(record.value());
                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);
                    try {
                        IndexResponse indexResponse =
                                openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        LOG.info("Index ResponseId :: {}", indexResponse.getId());
                    } catch (Exception e) {
                        //e.printStackTrace();
                    }
                });
            }
        }
    }

    private static String extractId(String json){
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}
