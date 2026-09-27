package com.example.fraud.rules;

import java.io.Serializable;

public record RuleConfig(
        double highAmountThreshold,
        int rapidTransactionCount,
        long rapidWindowMillis,
        long countryChangeWindowMillis) implements Serializable {

    public static RuleConfig fromEnvironment() {
        return new RuleConfig(
                doubleEnv("HIGH_AMOUNT_THRESHOLD", 100_000_000d),
                intEnv("RAPID_TRANSACTION_COUNT", 5),
                longEnv("RAPID_WINDOW_SECONDS", 10) * 1_000L,
                longEnv("COUNTRY_CHANGE_WINDOW_SECONDS", 120) * 1_000L);
    }

    private static double doubleEnv(String name, double fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Double.parseDouble(value);
    }

    private static int intEnv(String name, int fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value);
    }

    private static long longEnv(String name, long fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Long.parseLong(value);
    }
}
