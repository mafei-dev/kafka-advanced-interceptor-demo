package com.example.kafkaadvancedinterceptordemo.controller;

import com.example.kafkaadvancedinterceptordemo.payload.SagaPayload;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/sample")
public class SampleController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public SampleController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/{q}")
    public void invoke(@PathVariable Integer q) {
        for (int i = 0; i < q; i++) {
            SagaPayload sagaPayload = new SagaPayload();
            sagaPayload.setErrorDetail("" + i);
            this.kafkaTemplate.send("TEST_BULK_TOPIC", "");
        }
    }
}
