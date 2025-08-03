package com.example.kafkaadvancedinterceptordemo.endpoint;

import com.example.kafkaadvancedinterceptordemo.exception.RetryableException;
import com.example.kafkaadvancedinterceptordemo.exception.SagaNonRetryableException;
import com.example.kafkaadvancedinterceptordemo.payload.SagaPayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;

@Slf4j
public abstract class SampleEndpoint {
    @Qualifier("sagaAsyncRetryTemplate")
    @Autowired
    RetryTemplate retryTemplate;
    @Autowired
    @Qualifier("stacksagaKafkaAsyncTaskExecutor")
    TaskExecutor taskExecutor;

    protected abstract void onMessage(ConsumerRecord<String, SagaPayload> record) throws SagaNonRetryableException, RetryableException;

    protected void doProcessAsyncInAction(ConsumerRecord<String, SagaPayload> record) throws SagaNonRetryableException, RetryableException {
        log.info("doProcessAsyncInAction:default");
    }

    public final void doProcessAsync(ConsumerRecord<String, SagaPayload> consumerRecord) {
        this.taskExecutor.execute(() -> {
            this.doProcessAsyncInActionInternal(consumerRecord);
        });
    }

    private void doProcessAsyncInActionInternal(ConsumerRecord<String, SagaPayload> consumerRecord) {
        log.info("doProcessAsyncInActionInternal");
        this.getRetryTemplate().execute((RetryCallback<String, RuntimeException>) context -> {
            System.out.println("==============================================");
            try {
                this.doProcessAsyncInAction(consumerRecord);
            } catch (SagaNonRetryableException e) {
                throw new RuntimeException(e);
            }
            return "";
        }, context -> {
            log.info("exhausted retry attempt");
            System.out.println("exhausted retry attempt");
            return null;
        });
    }

    @DltHandler
    public void aVoid(ConsumerRecord<String, SagaPayload> record) {
        System.out.println("DltHandler:payload = " + record.value());
    }

    protected RetryTemplate getRetryTemplate() {
        return this.retryTemplate;
    }
}
