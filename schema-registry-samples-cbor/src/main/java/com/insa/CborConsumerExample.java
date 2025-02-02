package com.insa;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.insa.kafka.serializers.yang.cbor.KafkaYangCborSchemaDeserializer;
import com.insa.kafka.serializers.yang.cbor.KafkaYangCborSchemaDeserializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Headers;
import org.yangcentral.yangkit.data.api.model.YangDataDocument;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static com.insa.kafka.serializers.yang.cbor.AbstractKafkaYangCborSchemaSerializer.SCHEMA_ID_KEY;

public class CborConsumerExample {
    public static void main(String[] args) {

        Properties consumerConfig = new Properties();

        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaYangCborSchemaDeserializer.class.getName());
        consumerConfig.setProperty(KafkaYangCborSchemaDeserializerConfig.YANG_CBOR_FAIL_INVALID_SCHEMA, "true");
        consumerConfig.setProperty(KafkaYangCborSchemaDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());
        consumerConfig.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, YangDataDocument> consumer = new KafkaConsumer<>(consumerConfig);
        String topic = "yang.tests";

        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            try {
                ConsumerRecords<String, YangDataDocument> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, YangDataDocument> r : records) {
                    // Headers
                    Headers headers = r.headers();
                    byte[] serializedSchemaId = headers.lastHeader(SCHEMA_ID_KEY).value();
                    int schemaId = ByteBuffer.wrap(serializedSchemaId).getInt();
                    System.out.println("[Header] Key: " + SCHEMA_ID_KEY + ", Value: " + schemaId);
                    byte[] serializedContentType = headers.lastHeader("content-type").value();
                    System.out.println("[Header] Key: " + "content-type" + ", Value: " + new String(serializedContentType));

                    // Values
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode;
                    jsonNode = mapper.readTree(r.value().getDocString());
                    System.out.println("Key : " + r.key() + ", Value : " + jsonNode + ", Offset : " + r.offset());
                }
            } catch (RecordDeserializationException e) {
                System.out.println("Error during deserialization : message is ignored");
                consumer.seek(e.topicPartition(), e.offset() + 1L);
            } catch (JsonProcessingException ignored) {
                System.out.println("Error reading Cbor");
            }
        }
    }
}
