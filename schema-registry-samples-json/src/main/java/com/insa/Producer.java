/*package com.insa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.insa.kafka.serializers.yang.KafkaYangSchemaSerializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import okhttp3.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.dom4j.DocumentException;
import org.yangcentral.yangkit.common.api.validate.ValidatorResult;
import org.yangcentral.yangkit.model.api.schema.YangSchemaContext;
import org.yangcentral.yangkit.parser.YangParserException;
import org.yangcentral.yangkit.parser.YangYinParser;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;

public class Producer {
  private static final String TOPIC = "yang.tests";
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String SCHEMA_REGISTRY = "http://localhost:8081/";
  private static OkHttpClient client = new OkHttpClient();
  public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

  public static String postSchema(String yangModule) throws IOException {
    String bodyStart = "{\"schemaType\": \"YANG\", \"schema\": \"";
    String bodyEnd = "\"}";
    String escaped = yangModule.replaceAll("\"", "\\\\\"");
    escaped = escaped.replaceAll("\n", " ");
    String bodyJson = bodyStart + escaped + bodyEnd;
    System.out.println("JSON:" + bodyJson);
    RequestBody body = RequestBody.create(JSON, bodyJson);
    Request request = new Request.Builder().url(SCHEMA_REGISTRY).post(body).build();
    Response response = client.newCall(request).execute();
    if (response.code() != 200){
      System.out.println("Code = " + response.code());
      System.out.println(response.body().string());
    }
    return response.body().string();
  }

  public static void main(String[] args) throws DocumentException, IOException, YangParserException, RestClientException {
    //https://github.com/network-analytics/draft-daisy-kafka-yang-integration/blob/main/YANG%20Schema%20registry%20integration.pdf

    // Parsing Yang modules
    URL yangUrl = Producer.class.getClassLoader().getResource("yang");
    String yangDir = yangUrl.getFile();

    // TODO: not sure if we need them parsed
    YangSchemaContext schemaContext = YangYinParser.parse(yangDir);
    ValidatorResult result = schemaContext.validate();
    System.out.println("Valid? " + result.isOk());
    System.out.println("Size modules = " + schemaContext.getModules().size());

    List<String> toRegister = new ArrayList<>(asList(
            "yang/ietf-inet-types@2021-02-22.yang"
//        "yang/ietf-yang-types@2023-01-23.yang",
//        "yang/ietf-restconf@2017-01-26.yang",
//        "yang/ietf-datastores@2018-02-14.yang"
//        "yang/insa-test@2023-09-05.yang"
//        "yang/ietf-yang-structure-ext@2020-06-17.yang"
//        "yang/ietf-interfaces@2018-02-20.yang"
//        "yang/ietf-yang-patch@2017-02-22.yang",
//        "yang/ietf-netconf-acm@2018-02-14.yang",
//        "yang/ietf-yang-schema-mount@2019-01-14.yang",
//        "yang/ietf-ip@2018-02-22.yang",
//        "yang/ietf-network-instance@2019-01-21.yang",
//        "yang/ietf-notification@2023-07-23.yang",
//        "yang/ietf-subscribed-notifications@2019-09-09.yang",
//        "yang/ietf-yang-push@2019-09-09.yang"
    ));
    for (String yangModule : toRegister) {
      URL url = Producer.class.getClassLoader().getResource(yangModule);
      String yangString = Resources.toString(url, StandardCharsets.UTF_8);
      postSchema(yangString);
    }

    SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(SCHEMA_REGISTRY, 100);
    for (String subject : schemaRegistryClient.getAllSubjects()) {
      System.out.println("Subject: " + subject);
    }

    InputStream jsonInputStream = Producer.class.getClassLoader().getResourceAsStream("json/valid.json");
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode yangPushNotification = objectMapper.readTree(jsonInputStream);

    // Configure Kafka producer
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaYangSchemaSerializer.class);

    // Create producer
    KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(props);

    // Send message to Kafka topic
    ProducerRecord<String, JsonNode> record = new ProducerRecord<>(TOPIC, yangPushNotification);

    producer.send(record, (metadata, exception) -> {
      if (exception == null) {
        System.out.println("Message envoyé avec succès. Offset : " + metadata.offset());
      } else {
        System.err.println("Erreur lors de l'envoi du message : " + exception.getMessage());
      }
    });

    // Fermer le producteur Kafka
    producer.close();
  }
}*/