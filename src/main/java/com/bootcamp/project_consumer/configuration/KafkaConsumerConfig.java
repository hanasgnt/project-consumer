package com.bootcamp.project_consumer.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

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

        // =========================
        // Producer khusus buat publish ke Dead Letter Topic (DLT).
        // =========================
        @Bean
        public ProducerFactory<String, Object> dltProducerFactory() {
                Map<String, Object> config = new HashMap<>();

                config.put(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                "localhost:9092");

                config.put(
                                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                StringSerializer.class);

                config.put(
                                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                JsonSerializer.class);

                return new DefaultKafkaProducerFactory<>(config);
        }

        @Bean
        public KafkaTemplate<String, Object> dltKafkaTemplate() {
                return new KafkaTemplate<>(dltProducerFactory());
        }

        // =========================
        // Error handler: retry 3x dengan jeda 1 detik, kalau masih gagal baru publish
        // ke DLT.
        // =========================
        @Bean
        public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> dltKafkaTemplate) {

                DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                                (KafkaOperations) dltKafkaTemplate);

                FixedBackOff backOff = new FixedBackOff(1000L, 3L);

                DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

                // Jangan retry error yang emang nggak akan pernah berhasil walau diulang
                // (bug data, bukan masalah sementara) — langsung ke DLT tanpa buang waktu
                // retry.
                errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);

                return errorHandler;
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> kafkaListenerContainerFactory(
                        DefaultErrorHandler errorHandler) {
                ConcurrentKafkaListenerContainerFactory<String, TransactionEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();

                factory.setConsumerFactory(consumerFactory());
                factory.setCommonErrorHandler(errorHandler);

                return factory;
        }
}