package com.example.kafkaadvancedinterceptordemo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Configuration
public class PoolConfig {

    @Bean("customScheduler")
    public Scheduler customScheduler() {
        return Schedulers.newBoundedElastic(
                2,         // Max threads
                2,      // Task queue capacity
                "custom-elastic" // Thread name prefix
        );
    }
}
