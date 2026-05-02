package com.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import com.common.model.OrderEvent;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    // ConsumerFactory
    @Bean
    public ConsumerFactory<String, OrderEvent> consumerFactory() {

        JsonDeserializer<OrderEvent> deserializer =
                new JsonDeserializer<>(OrderEvent.class);

        deserializer.addTrustedPackages("*");

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                deserializer
        );
    }

    // KafkaTemplate (needed for DLQ)
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {

        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        ProducerFactory<String, Object> producerFactory =
                new DefaultKafkaProducerFactory<>(
                        props,
                        new org.apache.kafka.common.serialization.StringSerializer(),
                        new JsonSerializer<>()
                );

        return new KafkaTemplate<>(producerFactory);
    }

    // Listener Factory (fixed types)
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate());

        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3));

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}