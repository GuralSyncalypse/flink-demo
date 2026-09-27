package com.example.fraud.serialization;

import com.example.fraud.model.LateTransaction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class LateTransactionSerializationSchema implements SerializationSchema<LateTransaction> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(LateTransaction element) {
        try {
            return MAPPER.writeValueAsBytes(element);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Cannot serialize late transaction as JSON", exception);
        }
    }
}
