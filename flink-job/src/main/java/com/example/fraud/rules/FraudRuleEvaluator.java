package com.example.fraud.rules;

import com.example.fraud.model.FraudAlert;
import com.example.fraud.model.Transaction;

import java.util.Objects;
import java.util.UUID;
import java.nio.charset.StandardCharsets;

public final class FraudRuleEvaluator {
    private FraudRuleEvaluator() {
    }

    public static FraudAlert evaluate(
            Transaction current,
            Transaction previous,
            int transactionCountInWindow,
            RuleConfig config) {
        FraudAlert alert = baseAlert(current);

        if (current.amount >= config.highAmountThreshold()) {
            alert.riskScore += 60;
            alert.reasons.add("Số tiền giao dịch vượt ngưỡng");
        }

        if (transactionCountInWindow >= config.rapidTransactionCount()) {
            alert.riskScore += 55;
            alert.reasons.add("Nhiều giao dịch trong thời gian ngắn");
        }

        if (previous != null) {
            long elapsed = current.timestamp - previous.timestamp;
            boolean followsPrevious = elapsed >= 0;

            if (followsPrevious
                    && elapsed <= config.countryChangeWindowMillis()
                    && !Objects.equals(current.country, previous.country)) {
                alert.riskScore += 70;
                alert.reasons.add("Thay đổi quốc gia bất thường trong thời gian ngắn");
            }

            if (followsPrevious
                    && elapsed <= config.countryChangeWindowMillis()
                    && !Objects.equals(current.deviceId, previous.deviceId)) {
                alert.riskScore += 15;
                alert.reasons.add("Thiết bị vừa thay đổi");
            }
        }

        if (alert.riskScore < 50) {
            return null;
        }

        alert.severity = alert.riskScore >= 100 ? "CRITICAL"
                : alert.riskScore >= 70 ? "HIGH" : "MEDIUM";
        return alert;
    }

    private static FraudAlert baseAlert(Transaction tx) {
        FraudAlert alert = new FraudAlert();
        // Stable across replay, so downstream systems can de-duplicate at-least-once delivery.
        alert.alertId = UUID.nameUUIDFromBytes(tx.transactionId.getBytes(StandardCharsets.UTF_8)).toString();
        alert.transactionId = tx.transactionId;
        alert.accountId = tx.accountId;
        alert.amount = tx.amount;
        alert.currency = tx.currency;
        alert.country = tx.country;
        alert.merchantId = tx.merchantId;
        alert.deviceId = tx.deviceId;
        alert.transactionTimestamp = tx.timestamp;
        alert.detectedAt = System.currentTimeMillis();
        return alert;
    }
}
