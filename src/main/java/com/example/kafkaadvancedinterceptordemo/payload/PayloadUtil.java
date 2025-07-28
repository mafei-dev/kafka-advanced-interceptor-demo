package com.example.kafkaadvancedinterceptordemo.payload;

public class PayloadUtil {
    public static void setIdempotencyKey(SagaPayload sagaPayload, String idempotencyKey) {
        sagaPayload.setIdempotencyKey(idempotencyKey);
    }
}
