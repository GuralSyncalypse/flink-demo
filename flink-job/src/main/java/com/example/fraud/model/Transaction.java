package com.example.fraud.model;

import java.io.Serializable;

public class Transaction implements Serializable {
    public String transactionId;
    public String accountId;
    public double amount;
    public String currency;
    public String merchantId;
    public String merchantCategory;
    public String country;
    public String deviceId;
    public long timestamp;
    public String scenario;

    public Transaction() {
    }
}
