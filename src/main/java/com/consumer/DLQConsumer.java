package com.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DLQConsumer {
/*
    @KafkaListener(topics = "gi-topic1-dlq", groupId = "dlq-group")
    public void consumeDLQ(Object message) {
        System.out.println("DLQ Message: " + message);
    }*/
}