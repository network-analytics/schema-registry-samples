/*
 * Copyright 2025 INSA Lyon.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.insa.interfaces;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ch.swisscom.kafka.serializers.yang.json.KafkaYangJsonSchemaDeserializer;
import ch.swisscom.kafka.serializers.yang.json.KafkaYangJsonSchemaDeserializerConfig;
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

import static ch.swisscom.kafka.serializers.yang.cbor.AbstractKafkaYangCborSchemaSerializer.SCHEMA_ID_KEY;

public class JsonConsumerIetfInterfaces {

    public static String KAFKA_TOPIC = "yang.tests";

    public static void main(String[] args) {
        System.out.println("Starting Consumer");

        Properties consumerConfig = new Properties();

        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaYangJsonSchemaDeserializer.class.getName());
        consumerConfig.setProperty(KafkaYangJsonSchemaDeserializerConfig.YANG_JSON_FAIL_INVALID_SCHEMA, "true");
        consumerConfig.setProperty(KafkaYangJsonSchemaDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());
        consumerConfig.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Creating Consumer
        KafkaConsumer<String, YangDataDocument> consumer = new KafkaConsumer<>(consumerConfig);
        String topic = KAFKA_TOPIC;

        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            try {
                ConsumerRecords<String, YangDataDocument> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, YangDataDocument> r : records) {
                    System.out.println("********* JSON Message *********");
                    // Headers
                    Headers headers = r.headers();
                    byte[] serializedSchemaId = headers.lastHeader(SCHEMA_ID_KEY).value();
                    int schemaId = ByteBuffer.wrap(serializedSchemaId).getInt();
                    System.out.println("[Header] Key: " + SCHEMA_ID_KEY + ", Value: " + schemaId);
                    byte[] serializedContentType = headers.lastHeader("content-type").value();
                    System.out.println("[Header] Key: " + "content-type" + ", Value: " + new String(serializedContentType));

                    // Read JSON message
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode;
                    jsonNode = mapper.readTree(r.value().getDocString());
                    System.out.println("Key : " + r.key() + ", Value : " + jsonNode + ", Offset : " + r.offset());
                }
            } catch (RecordDeserializationException e) {
                System.out.println("Error during deserialization : message is ignored");
                consumer.seek(e.topicPartition(), e.offset() + 1L);
            } catch (JsonProcessingException ignored) {
                System.out.println("Error reading JSON");
            }
        }

    }
}
