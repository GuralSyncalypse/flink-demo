package com.example.fraud.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FraudAlert implements Serializable {
    public String alertId;
    public String transactionId;
    public String accountId;
    public double amount;
    public String currency;
    public String country;
    public String merchantId;
    public String deviceId;
    public long transactionTimestamp;
    public long detectedAt;
    public int riskScore;
    public String severity;
    public List<String> reasons = new ArrayList<>();

    public FraudAlert() {
    }
}
