package com.insa;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import fr.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaDeserializer;
import fr.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaDeserializerConfig;
import fr.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaSerializer;
import fr.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaSerializerConfig;
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
import org.dom4j.DocumentException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.yangcentral.yangkit.common.api.validate.ValidatorResult;
import org.yangcentral.yangkit.common.api.validate.ValidatorResultBuilder;
import org.yangcentral.yangkit.data.api.model.YangDataDocument;
import org.yangcentral.yangkit.data.codec.json.YangDataDocumentJsonParser;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.parser.YangYinParser;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Integration test for simple the producer and consumer.
 * The execution of the integration tests need a schema registry instance deployed.
 * Execute these tests only in controlled environments.
 * Enable test at your own risk.
 */
public class AppTestJson {

    private final static String TOPIC = "yang.tests";
    private final static String TOPIC_ERROR = "yang.tests.error";
    public static final String PRODUCER_MSG_ERROR_EXPECTED_THROW = "serializer does not throw serialization error when json is invalid";
    public static final String ERROR_TRYING_TO_SEND_DATA = "error trying to send data";
    public static final String JSON_NODE_AND_CONSUMER_JSON_NODE_ARE_DIFFERENT = "producer JsonNode and consumer JsonNode are different";
    public static final String ERROR_TRYING_TO_GET_DATA = "error trying to get data";
    public static final String CONSUMER_MSG_ERROR_EXPECTED_THROW = "deserializer does not throw deserialization error when json is invalid ";

    private YangSchemaContext getSchemaContext(String yangFile) {
        try {
            YangSchemaContext schemaContext = YangYinParser.parse(yangFile);
            ValidatorResult result = schemaContext.validate();
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
        return new YangDataDocumentJsonParser(schemaContext).parse(jsonNode, new ValidatorResultBuilder());
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
        ProducerRecord<String, YangDataDocument> record = new ProducerRecord<>(topic, "KEY", yangDataDocument);
        try {
            producer.send(record);
        } finally {
            producer.flush();
            producer.close();
        }
        return jsonNode;
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
            System.out.println("modules " + record.value().getSchemaContext().getModules());
            System.out.println("offset -> " + record.offset());
            System.out.println("json -> " + jsonNode);
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
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 1 module (insa-test) , Json : valid, Producer : valid true, Consumer : valid true")
    public void test1() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        JsonNode producerNode = assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test1/test.yang").getFile(),
                this.getClass().getClassLoader().getResource("json/test1/valid.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        JsonNode consumerNode = assertDoesNotThrow(() -> consumerGetLast(consumerProperties), ERROR_TRYING_TO_GET_DATA);
        assertEquals(producerNode, consumerNode, JSON_NODE_AND_CONSUMER_JSON_NODE_ARE_DIFFERENT);
    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 1 module (insa-test) , Json : invalid, Producer : valid true, Consumer : valid true")
    public void test2() {
        Properties producerProperties = getDefaultProducerConfig();
        assertThrowsExactly(SerializationException.class, () -> {
            producerSendJson(
                    this.getClass().getClassLoader().getResource("json/test2/test.yang").getFile(),
                    this.getClass().getClassLoader().getResource("json/test2/invalid.json").getFile(),
                    producerProperties,
                    TOPIC_ERROR
            );
        }, PRODUCER_MSG_ERROR_EXPECTED_THROW);
    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 1 module (insa-test) , Json : invalid, Producer : valid false, Consumer : valid true")
    public void test3() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        producerProperties.setProperty(KafkaYangJsonSchemaSerializerConfig.YANG_JSON_FAIL_INVALID_SCHEMA, "false");
        assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test3/test.yang").getFile(),
                this.getClass().getClassLoader().getResource("json/test3/invalid.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        assertThrowsExactly(RecordDeserializationException.class, () -> consumerGetLast(consumerProperties), CONSUMER_MSG_ERROR_EXPECTED_THROW);
    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 2 module (insa-test-simple-remote, insa-test-complex-remote) , Json : valid, Producer : valid true, Consumer : valid true")
    public void test4() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        //simple schema and simple data
        JsonNode producerNode = assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test4/insa-test-simple-remote.yang").getFile(),
                this.getClass().getClassLoader().getResource("json/test4/simple.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        JsonNode consumerNode = assertDoesNotThrow(() -> consumerGetLast(consumerProperties), ERROR_TRYING_TO_GET_DATA);
        assertEquals(producerNode, consumerNode, "producer JsonNode and consumer JsonNode are different");

        //complex schema and complex data
        producerNode = assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test4/insa-test-complex-remote.yang").getFile(),
                this.getClass().getClassLoader().getResource("json/test4/valid.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        consumerNode = assertDoesNotThrow(() -> consumerGetLast(consumerProperties), ERROR_TRYING_TO_GET_DATA);
        assertEquals(producerNode, consumerNode, JSON_NODE_AND_CONSUMER_JSON_NODE_ARE_DIFFERENT);
    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 2 module (insa-test-simple-remote, insa-test-complex-remote) , Json : valid, Producer : valid true, Consumer : valid true")
    public void test5() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        //simple schema and simple data
        JsonNode producerNode = assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test4/insa-test-simple-remote.yang").getFile(),
                this.getClass().getClassLoader().getResource("json/test4/simple.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        JsonNode consumerNode = assertDoesNotThrow(() -> consumerGetLast(consumerProperties), ERROR_TRYING_TO_GET_DATA);
        assertEquals(producerNode, consumerNode, JSON_NODE_AND_CONSUMER_JSON_NODE_ARE_DIFFERENT);

        //complex schema and complex data
        assertThrowsExactly(SerializationException.class, () -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test5/insa-test-complex-remote.yang").getFile(),
                this.getClass().getClassLoader().getResource("json/test5/invalid.json").getFile(),
                producerProperties,
                TOPIC_ERROR
        ), PRODUCER_MSG_ERROR_EXPECTED_THROW);
    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 2 module (insa-test-simple-remote, insa-test-complex-remote) , Json : valid, Producer : valid false (only for complex), Consumer : valid true")
    public void test6() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        //simple schema and simple data
        JsonNode producerNode = assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test6/insa-test-simple-remote.yang").getFile(),
                this.getClass().getClassLoader().getResource("json/test6/simple.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        JsonNode consumerNode = assertDoesNotThrow(() -> consumerGetLast(consumerProperties), ERROR_TRYING_TO_GET_DATA);
        assertEquals(producerNode, consumerNode, JSON_NODE_AND_CONSUMER_JSON_NODE_ARE_DIFFERENT);

        producerProperties.setProperty(KafkaYangJsonSchemaSerializerConfig.YANG_JSON_FAIL_INVALID_SCHEMA, "false");
        //complex schema and complex data
        assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test6/insa-test-complex-remote.yang").getFile(),
                this.getClass().getClassLoader().getResource("json/test6/invalid.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        assertThrowsExactly(RecordDeserializationException.class, () -> consumerGetLast(consumerProperties), CONSUMER_MSG_ERROR_EXPECTED_THROW);

    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 2 module (insa-test-simple-local, insa-test-complex-local), Json : valid, Producer : valid true, Consumer : valid true")
    public void test7() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        JsonNode producerNode = assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test7/yangs").getFile(),
                this.getClass().getClassLoader().getResource("json/test7/valid.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        JsonNode consumerNode = assertDoesNotThrow(() -> consumerGetLast(consumerProperties), ERROR_TRYING_TO_GET_DATA);
        assertEquals(producerNode, consumerNode, JSON_NODE_AND_CONSUMER_JSON_NODE_ARE_DIFFERENT);
    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 2 module (insa-test-simple-local, insa-test-complex-local), Json : invalid, Producer : valid true, Consumer : valid true")
    public void test8() {
        Properties producerProperties = getDefaultProducerConfig();
        assertThrowsExactly(SerializationException.class, () -> {
            producerSendJson(
                    this.getClass().getClassLoader().getResource("json/test8/yangs").getFile(),
                    this.getClass().getClassLoader().getResource("json/test8/invalid.json").getFile(),
                    producerProperties,
                    TOPIC_ERROR
            );
        }, PRODUCER_MSG_ERROR_EXPECTED_THROW);
    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 2 module (insa-test-simple-local, insa-test-complex-local), Json : invalid, Producer : valid false, Consumer : valid true")
    public void test9() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        producerProperties.setProperty(KafkaYangJsonSchemaSerializerConfig.YANG_JSON_FAIL_INVALID_SCHEMA, "false");
        assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test9/yangs").getFile(),
                this.getClass().getClassLoader().getResource("json/test9/invalid.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        assertThrowsExactly(RecordDeserializationException.class, () -> consumerGetLast(consumerProperties), CONSUMER_MSG_ERROR_EXPECTED_THROW);
    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 3 module (insa-test-base, insa-test-augments-1, insa-test-augments-2), Json : valid, Producer : valid true, Consumer : valid true")
    public void test10() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        JsonNode producerNode = assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test10/yangs").getFile(),
                this.getClass().getClassLoader().getResource("json/test10/valid.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        JsonNode consumerNode = assertDoesNotThrow(() -> consumerGetLast(consumerProperties), ERROR_TRYING_TO_GET_DATA);
        assertEquals(producerNode, consumerNode, JSON_NODE_AND_CONSUMER_JSON_NODE_ARE_DIFFERENT);
    }

    @Test
    @Disabled("Integration test need a deployed schema registry instance. Enable test only in a controlled environment.")
    @DisplayName("Yang : 3 module (insa-test-base-1, insa-test-augments-1, insa-test-base-2), Json : valid, Producer : valid true, Consumer : valid true")
    public void test11() {
        Properties producerProperties = getDefaultProducerConfig();
        Properties consumerProperties = getDefaultConsumerConfig();
        JsonNode producerNode = assertDoesNotThrow(() -> producerSendJson(
                this.getClass().getClassLoader().getResource("json/test11/yangs").getFile(),
                this.getClass().getClassLoader().getResource("json/test11/valid.json").getFile(),
                producerProperties,
                TOPIC
        ), ERROR_TRYING_TO_SEND_DATA);
        JsonNode consumerNode = assertDoesNotThrow(() -> consumerGetLast(consumerProperties), ERROR_TRYING_TO_GET_DATA);
        assertEquals(producerNode, consumerNode, JSON_NODE_AND_CONSUMER_JSON_NODE_ARE_DIFFERENT);
    }
}
