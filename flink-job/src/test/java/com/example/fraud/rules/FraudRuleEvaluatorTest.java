package com.example.fraud.rules;

import com.example.fraud.model.FraudAlert;
import com.example.fraud.model.Transaction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FraudRuleEvaluatorTest {
    private static final RuleConfig CONFIG = new RuleConfig(100_000_000d, 5, 10_000L, 120_000L);

    @Test
    void normalTransactionDoesNotCreateAlert() {
        Transaction transaction = tx("tx-normal", 2_000_000d, "VN", "phone-a", 1_000_000L);

        FraudAlert alert = FraudRuleEvaluator.evaluate(transaction, null, 1, CONFIG);

        assertNull(alert);
    }

    @Test
    void highAmountCreatesMediumAlert() {
        Transaction transaction = tx("tx-large", 150_000_000d, "VN", "phone-a", 1_000_000L);

        FraudAlert alert = FraudRuleEvaluator.evaluate(transaction, null, 1, CONFIG);

        assertNotNull(alert);
        assertEquals(60, alert.riskScore);
        assertEquals("MEDIUM", alert.severity);
        assertTrue(alert.reasons.contains("Số tiền giao dịch vượt ngưỡng"));
    }

    @Test
    void rapidTransactionsCreateAlertAtConfiguredCount() {
        Transaction transaction = tx("tx-burst", 100_000d, "VN", "phone-a", 1_005_000L);

        FraudAlert alert = FraudRuleEvaluator.evaluate(transaction, null, 5, CONFIG);

        assertNotNull(alert);
        assertEquals(55, alert.riskScore);
        assertTrue(alert.reasons.get(0).contains("thời gian ngắn"));
    }

    @Test
    void fastCountryAndDeviceChangeCreatesHighAlert() {
        Transaction previous = tx("tx-before", 100_000d, "VN", "phone-a", 1_000_000L);
        Transaction current = tx("tx-after", 200_000d, "US", "phone-b", 1_030_000L);

        FraudAlert alert = FraudRuleEvaluator.evaluate(current, previous, 2, CONFIG);

        assertNotNull(alert);
        assertEquals(85, alert.riskScore);
        assertEquals("HIGH", alert.severity);
        assertEquals(2, alert.reasons.size());
    }

    @Test
    void oldCountryChangeIsNotSuspicious() {
        Transaction previous = tx("tx-before", 100_000d, "VN", "phone-a", 1_000_000L);
        Transaction current = tx("tx-after", 200_000d, "US", "phone-b", 1_121_000L);

        assertNull(FraudRuleEvaluator.evaluate(current, previous, 2, CONFIG));
    }

    @Test
    void alertIdIsStableForReplay() {
        Transaction transaction = tx("same-transaction", 150_000_000d, "VN", "phone-a", 1_000_000L);

        FraudAlert first = FraudRuleEvaluator.evaluate(transaction, null, 1, CONFIG);
        FraudAlert replayed = FraudRuleEvaluator.evaluate(transaction, null, 1, CONFIG);

        assertNotNull(first);
        assertNotNull(replayed);
        assertEquals(first.alertId, replayed.alertId);
    }

    private static Transaction tx(String id, double amount, String country, String device, long timestamp) {
        Transaction tx = new Transaction();
        tx.transactionId = id;
        tx.accountId = "ACC-001";
        tx.amount = amount;
        tx.currency = "VND";
        tx.merchantId = "MERCHANT-001";
        tx.country = country;
        tx.deviceId = device;
        tx.timestamp = timestamp;
        return tx;
    }
}
