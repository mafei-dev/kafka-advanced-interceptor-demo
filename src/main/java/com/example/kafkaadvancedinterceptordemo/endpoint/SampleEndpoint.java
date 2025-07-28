package com.example.kafkaadvancedinterceptordemo.endpoint;

import com.example.kafkaadvancedinterceptordemo.payload.SagaPayload;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;

public class SampleEndpoint {
    @DltHandler
    public void aVoid(ConsumerRecord<String, SagaPayload> record) {
        System.out.println("DltHandler:payload = " + record.value());
    }
}
