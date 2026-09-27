package com.example.fraud.serialization;

import com.example.fraud.model.Transaction;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class TransactionDeserializationSchema extends AbstractDeserializationSchema<Transaction> {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, Transaction.class);
    }
}
