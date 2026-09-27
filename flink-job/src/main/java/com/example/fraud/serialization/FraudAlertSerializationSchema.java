package com.example.fraud.serialization;

import com.example.fraud.model.FraudAlert;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class FraudAlertSerializationSchema implements SerializationSchema<FraudAlert> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(FraudAlert element) {
        try {
            return MAPPER.writeValueAsBytes(element);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Không thể chuyển cảnh báo thành JSON", exception);
        }
    }
}
