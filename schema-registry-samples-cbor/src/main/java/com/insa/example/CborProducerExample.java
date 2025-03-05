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

package com.insa.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.insa.kafka.serializers.yang.cbor.KafkaYangCborSchemaSerializer;
import com.insa.kafka.serializers.yang.cbor.KafkaYangCborSchemaSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.dom4j.DocumentException;
import org.yangcentral.yangkit.common.api.validate.ValidatorResultBuilder;
import org.yangcentral.yangkit.data.api.model.YangDataDocument;
import org.yangcentral.yangkit.data.codec.json.YangDataDocumentJsonParser;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.parser.YangYinParser;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class CborProducerExample {

    public static String KAFKA_TOPIC = "yang.tests";

    public static void main(String[] args) throws DocumentException, IOException, YangParserException {

        // Configure Kafka producer
        Properties producerConfig = new Properties();

        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaYangCborSchemaSerializer.class.getName());
        producerConfig.setProperty(KafkaYangCborSchemaSerializerConfig.YANG_CBOR_FAIL_INVALID_SCHEMA, "true");
        producerConfig.setProperty(KafkaYangCborSchemaSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());

        producerConfig.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        // Create producer
        KafkaProducer<String, YangDataDocument> producer = new KafkaProducer<>(producerConfig);

        YangSchemaContext schemaContext = YangYinParser.parse(CborProducerExample.class.getClassLoader().getResource("example/test.yang").getFile());
        schemaContext.validate();
        JsonNode jsonNode = new ObjectMapper().readTree(new File(CborProducerExample.class.getClassLoader().getResource("example/valid.json").getFile()));
        ValidatorResultBuilder validatorResultBuilder = new ValidatorResultBuilder();
        YangDataDocument doc = new YangDataDocumentJsonParser(schemaContext).parse(jsonNode, validatorResultBuilder);
        doc.validate();

        String key = "key1";
        String topic = KAFKA_TOPIC;
        ProducerRecord<String, YangDataDocument> record = new ProducerRecord<>(topic, key, doc);

        try {
            producer.send(record);
        } catch (SerializationException e) {
            System.out.println("Error serializing: " + e.getMessage());
        } finally {
            producer.flush();
            producer.close();
        }

        System.out.println("sent -> " + jsonNode);
    }
}