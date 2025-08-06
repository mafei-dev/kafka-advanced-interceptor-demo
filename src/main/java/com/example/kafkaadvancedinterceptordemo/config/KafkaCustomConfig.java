package com.example.kafkaadvancedinterceptordemo.config;

import com.example.kafkaadvancedinterceptordemo.exception.RetryableException;
import com.example.kafkaadvancedinterceptordemo.exception.SagaNonRetryableException;
import com.example.kafkaadvancedinterceptordemo.payload.PayloadUtil;
import com.example.kafkaadvancedinterceptordemo.payload.SagaPayload;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.backoff.FixedBackOff;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
@AutoConfigureAfter(KafkaAutoConfiguration.class)
public class KafkaCustomConfig {


    @Bean("sagaConsumerFactory")
    ConsumerFactory<String, SagaPayload> sagaConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.remove(JsonDeserializer.TRUSTED_PACKAGES);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafkaadvancedinterceptordemo");
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new JsonDeserializer<>(SagaPayload.class, false));
    }

    @Bean("sagaProducerFactory")
    public ProducerFactory<String, SagaPayload> sagaProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean("sagaKafkaTemplate")
    public KafkaTemplate<String, SagaPayload> sagaKafkaTemplate(@Qualifier("sagaProducerFactory") ProducerFactory<String, SagaPayload> sagaPayloadProducerFactory) {
        return new KafkaTemplate<>(sagaPayloadProducerFactory);
    }

    @Bean("sagaKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, SagaPayload> sagaKafkaListenerContainerFactory(
            @Qualifier("sagaConsumerFactory")
            ConsumerFactory<String, SagaPayload> sagaConsumerFactory,
            @Qualifier("sagaKafkaTemplate")
            KafkaTemplate<String, SagaPayload> sagaPayloadKafkaTemplate,
            @Qualifier("stacksagaKafkaAsyncTaskExecutor")
            ThreadPoolTaskExecutor taskExecutor) {
        ConcurrentKafkaListenerContainerFactory<String, SagaPayload> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(sagaConsumerFactory);
        // Create a backoff policy (fixed delay of 2 seconds, max 3 attempts)
        FixedBackOff fixedBackOff = new FixedBackOff(2000L, 3L);
        // Create DefaultErrorHandler with backoff
        DefaultErrorHandler errorHandler = new DefaultErrorHandler((consumerRecord, exception) -> {
            System.out.println("i am lost");
        }, fixedBackOff);
        errorHandler.addRetryableExceptions(RetryableException.class);
        errorHandler.addNotRetryableExceptions(SagaNonRetryableException.class, RuntimeException.class);
        factory.setReplyTemplate(sagaPayloadKafkaTemplate);
        factory.setCommonErrorHandler(errorHandler);
        factory.setRecordInterceptor(new RecordInterceptor<String, SagaPayload>() {
            @Override
            public ConsumerRecord<String, SagaPayload> intercept(ConsumerRecord<String, SagaPayload> record, Consumer<String, SagaPayload> consumer) {
                System.out.println("intercepting... ");
                PayloadUtil.setIdempotencyKey(record.value(), new String(record.headers().lastHeader("idempotencyKey").value()));
                record.headers().add(new RecordHeader("same", LocalDateTime.now().toString().getBytes()));
                return record;
            }

            @Override
            public void success(ConsumerRecord<String, SagaPayload> record, Consumer<String, SagaPayload> consumer) {
                System.out.println("success... ");
            }

            @Override
            public void afterRecord(ConsumerRecord<String, SagaPayload> record, Consumer<String, SagaPayload> consumer) {
                System.out.println("afterRecord.. ");
            }

            @Override
            public void failure(ConsumerRecord<String, SagaPayload> record, Exception exception, Consumer<String, SagaPayload> consumer) {

            }
        });
        return factory;
    }

    @Bean("sagaAsyncRetryTemplate")
    public RetryTemplate createRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // ✅ Retry Policy (e.g., retry max 3 times for any Exception)
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(5);
        retryTemplate.setRetryPolicy(retryPolicy);

        // ✅ Backoff Policy (e.g., wait 2 seconds between retries)
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(1000);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        return retryTemplate;
    }
    @Bean("stacksagaKafkaAsyncTaskExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);           // number of core threads
        executor.setMaxPoolSize(10);           // max threads
        executor.setQueueCapacity(50);         // queue before creating extra threads
        executor.setThreadNamePrefix("saga-async-");
        executor.initialize();                 // IMPORTANT: start the executor
        return executor;
    }
}
