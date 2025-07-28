package com.example.kafkaadvancedinterceptordemo.listner;

import com.example.kafkaadvancedinterceptordemo.annotation.SagaKafkaListener;
import com.example.kafkaadvancedinterceptordemo.endpoint.SampleEndpoint;
import com.example.kafkaadvancedinterceptordemo.exception.SagaNonRetryableException;
import com.example.kafkaadvancedinterceptordemo.payload.SagaPayload;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
public class SampleTopicListener extends SampleEndpoint {


    @SagaKafkaListener("TEST_TOPIC")
    @RetryableTopic(attempts = "2", backoff = @Backoff(delay = 2000), dltStrategy = DltStrategy.FAIL_ON_ERROR)
    public void onMessage(ConsumerRecord<String, SagaPayload> record) throws SagaNonRetryableException {
        System.out.println("payload = " + record.value());
        System.out.println("getIdempotencyKey = " + record.value().getIdempotencyKey());
        record.headers().forEach(header -> {
            System.out.println(header.key() + " = " + new String(header.value()));
        });
        throw new RuntimeException();
    }

}
