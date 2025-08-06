package com.example.kafkaadvancedinterceptordemo.annotation;

import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@KafkaListener(
        containerFactory = "sagaKafkaListenerContainerFactory",
        groupId = "${spring.application.name}"
)
public @interface SagaKafkaListener {
    @AliasFor(annotation = KafkaListener.class, attribute = "topics")
    String value();

    @AliasFor(annotation = KafkaListener.class, attribute = "id")
    String id();

    @AliasFor(annotation = KafkaListener.class, attribute = "concurrency")
    String concurrency() default "";

    @AliasFor(annotation = KafkaListener.class, attribute = "properties")
    String[] properties() default {};
}
