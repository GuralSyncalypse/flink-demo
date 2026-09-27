package com.example.fraud.rules;

import com.example.fraud.model.FraudAlert;
import com.example.fraud.model.LateTransaction;
import com.example.fraud.model.Transaction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class FraudDetector extends KeyedProcessFunction<String, Transaction, FraudAlert> {
    public static final OutputTag<LateTransaction> LATE_TRANSACTIONS =
            new OutputTag<LateTransaction>("late-transactions") { };

    private final RuleConfig config;
    private transient ValueState<Transaction> previousTransaction;
    private transient ListState<Long> recentTimestamps;
    private transient ValueState<Long> cleanupTimer;
    private transient Counter processedCounter;
    private transient Counter alertCounter;
    private transient Counter lateCounter;
    private transient Counter outOfOrderCounter;

    public FraudDetector(RuleConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        previousTransaction = getRuntimeContext().getState(
                new ValueStateDescriptor<>("previous-transaction", Transaction.class));
        recentTimestamps = getRuntimeContext().getListState(
                new ListStateDescriptor<>("recent-timestamps", Long.class));
        cleanupTimer = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cleanup-timer", Long.class));
        processedCounter = getRuntimeContext().getMetricGroup().counter("transactions_processed");
        alertCounter = getRuntimeContext().getMetricGroup().counter("fraud_alerts_emitted");
        lateCounter = getRuntimeContext().getMetricGroup().counter("late_events");
        outOfOrderCounter = getRuntimeContext().getMetricGroup().counter("out_of_order_events");
    }

    @Override
    public void processElement(Transaction tx, Context context, Collector<FraudAlert> output) throws Exception {
        processedCounter.inc();

        long watermark = context.timerService().currentWatermark();
        if (watermark != Long.MIN_VALUE && tx.timestamp < watermark) {
            lateCounter.inc();
            context.output(
                    LATE_TRANSACTIONS,
                    LateTransaction.from(tx, watermark, context.timerService().currentProcessingTime()));
            return;
        }

        Transaction previous = previousTransaction.value();
        if (previous != null && tx.timestamp < previous.timestamp) {
            outOfOrderCounter.inc();
        }

        List<Long> retained = new ArrayList<>();
        int countInWindow = 1; // Include the current transaction.
        long windowStart = tx.timestamp - config.rapidWindowMillis();

        for (Long timestamp : recentTimestamps.get()) {
            if (timestamp >= windowStart) {
                retained.add(timestamp);
                if (timestamp <= tx.timestamp) {
                    countInWindow++;
                }
            }
        }
        retained.add(tx.timestamp);
        recentTimestamps.update(retained);

        FraudAlert alert = FraudRuleEvaluator.evaluate(tx, previous, countInWindow, config);
        if (alert != null) {
            alertCounter.inc();
            output.collect(alert);
        }

        if (previous == null || tx.timestamp >= previous.timestamp) {
            previousTransaction.update(tx);
            scheduleCleanup(tx.timestamp, context);
        }
    }

    private void scheduleCleanup(long eventTimestamp, Context context) throws Exception {
        Long previousTimer = cleanupTimer.value();
        if (previousTimer != null) {
            context.timerService().deleteEventTimeTimer(previousTimer);
        }
        long retentionMillis = Math.max(config.rapidWindowMillis(), config.countryChangeWindowMillis());
        long timer = eventTimestamp + retentionMillis + 5_000L;
        context.timerService().registerEventTimeTimer(timer);
        cleanupTimer.update(timer);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<FraudAlert> output) throws Exception {
        Long activeTimer = cleanupTimer.value();
        if (activeTimer != null && activeTimer == timestamp) {
            previousTransaction.clear();
            recentTimestamps.clear();
            cleanupTimer.clear();
        }
    }
}
