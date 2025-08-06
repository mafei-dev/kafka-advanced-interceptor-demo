package com.example.kafkaadvancedinterceptordemo.listner;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.List;

@Component
public class SampleBulkListener {
    private final DatabaseClient databaseClient;
    private final Scheduler scheduler;

    public SampleBulkListener(DatabaseClient databaseClient, @Qualifier("customScheduler") Scheduler scheduler) {
        this.databaseClient = databaseClient;
        this.scheduler = scheduler;
    }

    @KafkaListener(topics = "TEST_BULK_TOPIC")
    public void listener(List<ConsumerRecord<String, String>> consumerRecord) {
        Mono.just(consumerRecord)
                .doOnNext(record -> {
                    System.out.println("before:" + Thread.currentThread().getName());
                })
                .flatMap(consumerRecords -> {
                    return databaseClient.sql("SELECT SLEEP(10);")
                            .fetch()
                            .rowsUpdated();
                })
                .subscribeOn(this.scheduler)
                .doOnNext(record -> {
                    System.out.println("after:" + Thread.currentThread().getName());
                })
                .subscribe();
        System.out.println("==================================");
        /*for (ConsumerRecord<String, SagaPayload> record : consumerRecords) {

        }*/
    }
}
