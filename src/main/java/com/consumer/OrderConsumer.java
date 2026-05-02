package com.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.common.model.OrderEvent;

@Service
public class OrderConsumer {

    @KafkaListener(topics = "gi-topic3", groupId = "order-group")
    public void consume(OrderEvent event) {

        System.out.println("Received: " + event);
        // simulate failure
        if ("fail".equals(event.getStatus())) {
            throw new RuntimeException("Error processing message");
        
        }
    }
}