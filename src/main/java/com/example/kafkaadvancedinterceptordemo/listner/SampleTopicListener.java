package com.example.kafkaadvancedinterceptordemo.listner;

import com.example.kafkaadvancedinterceptordemo.annotation.SagaKafkaListener;
import com.example.kafkaadvancedinterceptordemo.endpoint.SampleEndpoint;
import com.example.kafkaadvancedinterceptordemo.exception.RetryableException;
import com.example.kafkaadvancedinterceptordemo.exception.SagaNonRetryableException;
import com.example.kafkaadvancedinterceptordemo.payload.SagaPayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SampleTopicListener extends SampleEndpoint {


    @SagaKafkaListener(value = "TEST_TOPIC", id = "TEST_TOPIC_ID")
//    @RetryableTopic(attempts = "2", backoff = @Backoff(delay = 2000), dltStrategy = DltStrategy.FAIL_ON_ERROR)
    public void onMessage(ConsumerRecord<String, SagaPayload> record) throws SagaNonRetryableException, SagaNonRetryableException {
        log.info("onMessage");
        this.doProcessAsync(record);
    }

    @Override
    protected void doProcessAsyncInAction(ConsumerRecord<String, SagaPayload> record) throws SagaNonRetryableException, RetryableException {
        log.info("doProcessAsyncInAction");
//        System.out.println("payload = " + record.value());
//        System.out.println("getIdempotencyKey = " + record.value().getIdempotencyKey());
        record.headers().forEach(header -> {
            System.out.println(header.key() + " = " + new String(header.value()));
        });
        throw new RuntimeException();

    }

    @Override
    protected RetryTemplate getRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        // ✅ Retry Policy (e.g., retry max 3 times for any Exception)
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(1);
        retryTemplate.setRetryPolicy(retryPolicy);

        // ✅ Backoff Policy (e.g., wait 2 seconds between retries)
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(1000);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }
}
