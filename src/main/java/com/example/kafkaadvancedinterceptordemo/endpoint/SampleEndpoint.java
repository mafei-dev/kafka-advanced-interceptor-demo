package com.example.kafkaadvancedinterceptordemo.endpoint;

import com.example.kafkaadvancedinterceptordemo.annotation.SagaKafkaListener;
import com.example.kafkaadvancedinterceptordemo.exception.RetryableException;
import com.example.kafkaadvancedinterceptordemo.exception.SagaNonRetryableException;
import com.example.kafkaadvancedinterceptordemo.payload.SagaPayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;

@Slf4j
public abstract class SampleEndpoint {
    @Qualifier("sagaAsyncRetryTemplate")
    @Autowired
    RetryTemplate retryTemplate;
    @Autowired
    @Qualifier("stacksagaKafkaAsyncTaskExecutor")
    ThreadPoolTaskExecutor taskExecutor;
    ThreadPoolTaskExecutor realTaskExecutor;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    protected abstract void onMessage(ConsumerRecord<String, SagaPayload> record) throws SagaNonRetryableException, RetryableException;

    protected void doProcessAsyncInAction(ConsumerRecord<String, SagaPayload> record) throws SagaNonRetryableException, RetryableException {
        log.info("doProcessAsyncInAction:default");
    }

    public final void doProcessAsync(ConsumerRecord<String, SagaPayload> consumerRecord) throws RejectedExecutionException {
        try {
            Method onMessage = this.getClass().getMethod("onMessage", ConsumerRecord.class);
            SagaKafkaListener annotation = onMessage.getAnnotation(SagaKafkaListener.class);
            Objects.requireNonNull(registry.getListenerContainer(annotation.id())).pause();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

/*
        this.taskExecutor.execute(() -> {
            this.doProcessAsyncInActionInternal(consumerRecord);
        });
*/

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

    protected ThreadPoolTaskExecutor getThreadPoolTaskExecutor() {
        return this.taskExecutor;
    }

}
