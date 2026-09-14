package com.bootcamp.project_consumer.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.bootcamp.project_consumer.dto.TransactionEvent;

@Configuration
public class KafkaConsumerConfig {

        @Bean
        public ConsumerFactory<String, TransactionEvent> consumerFactory() {
                Map<String, Object> config = new HashMap<>();

                config.put(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                "localhost:9092");

                config.put(
                                ConsumerConfig.GROUP_ID_CONFIG,
                                "transaction-consumer-group");

                config.put(
                                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                StringDeserializer.class);

                config.put(
                                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                JsonDeserializer.class);

                config.put(
                                JsonDeserializer.TRUSTED_PACKAGES,
                                "com.bootcamp.project.dto,com.bootcamp.project_consumer.dto");

                config.put(
                                JsonDeserializer.VALUE_DEFAULT_TYPE,
                                "com.bootcamp.project_consumer.dto.TransactionEvent");

                return new DefaultKafkaConsumerFactory<>(
                                config,
                                new StringDeserializer(),
                                new JsonDeserializer<>(TransactionEvent.class));
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> kafkaListenerContainerFactory() {
                ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();

                factory.setConsumerFactory(consumerFactory());

                return factory;
        }
}