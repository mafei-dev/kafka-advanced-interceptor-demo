package com.example.kafkaadvancedinterceptordemo.payload;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class SagaPayload {
    private JsonNode aggregator;
    private String errorDetail;
    private String idempotencyKey;

    void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }
}
