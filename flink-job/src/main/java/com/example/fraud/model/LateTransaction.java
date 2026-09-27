package com.example.fraud.model;

import java.io.Serializable;

public class LateTransaction implements Serializable {
    public String transactionId;
    public String accountId;
    public long eventTimestamp;
    public long currentWatermark;
    public long latenessMillis;
    public long observedAt;
    public String scenario;
    public String reason;

    public LateTransaction() {
    }

    public static LateTransaction from(Transaction transaction, long watermark, long observedAt) {
        LateTransaction late = new LateTransaction();
        late.transactionId = transaction.transactionId;
        late.accountId = transaction.accountId;
        late.eventTimestamp = transaction.timestamp;
        late.currentWatermark = watermark;
        late.latenessMillis = Math.max(0L, watermark - transaction.timestamp);
        late.observedAt = observedAt;
        late.scenario = transaction.scenario;
        late.reason = "EVENT_TIME_BEHIND_WATERMARK";
        return late;
    }
}
