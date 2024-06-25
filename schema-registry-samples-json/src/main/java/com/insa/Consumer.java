package com.insa;

import com.insa.kafka.serializers.yang.cbor.KafkaYangCborSchemaDeserializer;
import com.insa.kafka.serializers.yang.cbor.KafkaYangCborSchemaSerializer;
import com.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaDeserializer;
import com.insa.kafka.serializers.yang.json.KafkaYangJsonSchemaSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.yangcentral.yangkit.data.api.model.YangDataDocument;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args){
        System.out.println("Consumer ->");

        Properties consumerConfig = new Properties();

        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.22.161.64:9092");
        consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaYangJsonSchemaDeserializer.class.getName());
        consumerConfig.setProperty("schema.registry.url", "http://172.22.161.64:8081");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerConfig);
        String topic = "topic.test";

        consumer.subscribe(Collections.singletonList(topic));

        while(true){
            ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<Object, Object> r: records){
                System.out.println("Clé : " + r.key() + ", Valeur : " + r.value() + ", Offset : " + r.offset());
            }
        }

    }
}
