package com.insa;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaDeserializer;
import com.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaDeserializerConfig;
import com.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaSerializer;
import com.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.Record;
import org.dom4j.DocumentException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.yangcentral.yangkit.base.Yang;
import org.yangcentral.yangkit.common.api.validate.ValidatorResultBuilder;
import org.yangcentral.yangkit.data.api.model.YangDataDocument;
import org.yangcentral.yangkit.data.codec.json.YangDataParser;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.parser.YangYinParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Unit test for simple App.
 */
public class AppTest {

    private final static String TOPIC = "yang.tests";
    private final static String TOPIC_ERROR = "yang.tests.error";
    private final String KEY = "KEY";

    private YangSchemaContext getSchemaContext(String yangFile) {
        try {
            YangSchemaContext schemaContext = YangYinParser.parse(yangFile);
            schemaContext.validate();
            return schemaContext;
        } catch (IOException | YangParserException | DocumentException e) {
            return null;
        }
    }

    private JsonNode getJsonNode(String jsonFile) {
        try {
            return new ObjectMapper().readTree(new File(jsonFile));
        } catch (IOException e) {
            return null;
        }
    }

    private YangDataDocument getYangDataDocument(YangSchemaContext schemaContext, JsonNode jsonNode) {
        return new YangDataParser(jsonNode, schemaContext, false).parse(new ValidatorResultBuilder());
    }

    private Properties getDefaultProducerConfig() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaYangJsonSchemaSerializer.class.getName());
        properties.setProperty(KafkaYangJsonSchemaSerializerConfig.YANG_JSON_FAIL_INVALID_SCHEMA, "true");
        properties.setProperty(KafkaYangJsonSchemaSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        return properties;
    }

    private Properties getDefaultConsumerConfig() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaYangJsonSchemaDeserializer.class.getName());
        properties.setProperty(KafkaYangJsonSchemaDeserializerConfig.YANG_JSON_FAIL_INVALID_SCHEMA, "true");
        properties.setProperty(KafkaYangJsonSchemaDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private JsonNode producerSendJson(String schemaFile, String jsonFile, Properties producerConfig, String topic) {
        YangSchemaContext schemaContext = getSchemaContext(schemaFile);
        assertNotNull(schemaContext, "SchemaContext is null, please check path");
        JsonNode jsonNode = getJsonNode(jsonFile);
        assertNotNull(jsonNode, "JsonNode is null, please check path");
        YangDataDocument yangDataDocument = getYangDataDocument(schemaContext, jsonNode);
        KafkaProducer<String, YangDataDocument> producer = new KafkaProducer<>(producerConfig);
        ProducerRecord<String, YangDataDocument> record = new ProducerRecord<>(topic, KEY, yangDataDocument);
        try {
            producer.send(record);
        } finally {
            producer.flush();
            producer.close();
        }
        return jsonNode.get("data");
    }

    private JsonNode consumerGetLast(Properties consumerConfig) {
        KafkaConsumer<String, YangDataDocument> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singletonList(TOPIC));
        JsonNode jsonNode;
        try {
            ConsumerRecords<String, YangDataDocument> records = consumer.poll(Duration.ofMillis(100));
            ObjectMapper mapper = new ObjectMapper();
            ConsumerRecord<String, YangDataDocument> record = records.iterator().next();
            jsonNode = mapper.readTree(record.value().getDocString());
            System.out.println("offset -> " + record.offset());
            System.out.println("json -> " +jsonNode);
        } catch (RecordDeserializationException e) {
            System.out.println("offset skip -> " + e.offset());
            consumer.seek(e.topicPartition(), e.offset() + 1L);
            throw e;
        } catch (JsonProcessingException e) {
            throw new RuntimeException();
        } finally {
            consumer.close();
        }
        return jsonNode;

    }

    @BeforeAll
    public static void cleanUpSchemaRegistry() throws IOException {
        System.out.println("CLEAN UP SCHEMA REGISTRY");

        URL url = new URL("http://localhost:8081/subjects");
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode subjects = (ArrayNode) objectMapper.readTree(url);
        for (JsonNode subject : subjects) {
            String subjectString = subject.asText();
            URL deleteUrl = new URL(url + "/" + subjectString);
            HttpURLConnection httpURLConnection = (HttpURLConnection) deleteUrl.openConnection();
            httpURLConnection.setRequestMethod("DELETE");
            httpURLConnection.getResponseCode();
        }

        System.out.println("CLEAN UP SCHEMA REGISTRY DONE");
    }

    @BeforeAll
    public static void cleanUpKafka() throws InterruptedException, ExecutionException {
        System.out.println("CLEAN UP KAFKA");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("client.id", "java-admin-client");
        AdminClient adminClient = AdminClient.create(properties);

        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);
        ListTopicsResult topics = adminClient.listTopics(options);
        Set<String> names;
        names = topics.names().get();
        List<String> topicsToDelete = new ArrayList<>();
        if (names.contains(TOPIC)) topicsToDelete.add(TOPIC);
        if (names.contains(TOPIC_ERROR)) topicsToDelete.add(TOPIC_ERROR);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicsToDelete);
        deleteTopicsResult.all().get();

        System.out.println("CLEAN UP KAFKA DONE");
    }

    @Test
    @DisplayName("Yang : 1 module (insa-test) 1 file , Json : valid, Producer : valid true, Consumer : valid true")
    public void test1() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        JsonNode producerNode = producerSendJson(
                this.getClass().getClassLoader().getResource("test1/test.yang").getFile(),
                this.getClass().getClassLoader().getResource("test1/valid.json").getFile(),
                producerProperties,
                TOPIC
        );
        assertNotNull(producerNode, "error trying to send data");
        JsonNode consumerNode = consumerGetLast(consumerProperties);
        assertNotNull(consumerNode, "error trying to get data");
        assertEquals(producerNode, consumerNode, "producer node and consumer node are different");
    }

    @Test
    @DisplayName("Yang : 1 module (insa-test) 1 file , Json : invalid, Producer : valid true, Consumer : valid true")
    public void test2() {
        Properties producerProperties = getDefaultProducerConfig();
        assertThrows(SerializationException.class, () -> {
            producerSendJson(
                    this.getClass().getClassLoader().getResource("test2/test.yang").getFile(),
                    this.getClass().getClassLoader().getResource("test2/invalid.json").getFile(),
                    producerProperties,
                    TOPIC_ERROR
            );
        }, "serializer does not throw error when json is invalid");
    }

    @Test
    @DisplayName("Yang : 1 module (insa-test) 1 file , Json : invalid, Producer : valid false, Consumer : valid true")
    public void test3() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        producerProperties.setProperty(KafkaYangJsonSchemaDeserializerConfig.YANG_JSON_FAIL_INVALID_SCHEMA, "false");
        assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("test3/test.yang").getFile(),
                this.getClass().getClassLoader().getResource("test3/invalid.json").getFile(),
                producerProperties,
                TOPIC
        ));
        assertThrowsExactly(RecordDeserializationException.class, () -> consumerGetLast(consumerProperties));
    }

}
