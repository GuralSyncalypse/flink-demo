package com.example.fraud;

import com.example.fraud.model.FraudAlert;
import com.example.fraud.model.LateTransaction;
import com.example.fraud.model.Transaction;
import com.example.fraud.rules.FraudDetector;
import com.example.fraud.rules.RuleConfig;
import com.example.fraud.serialization.FraudAlertSerializationSchema;
import com.example.fraud.serialization.LateTransactionSerializationSchema;
import com.example.fraud.serialization.TransactionDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.Properties;

public final class FraudDetectionJob {
    private FraudDetectionJob() {
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092");
        String transactionsTopic = env("TRANSACTIONS_TOPIC", "transactions");
        String alertsTopic = env("ALERTS_TOPIC", "fraud-alerts");
        String lateTransactionsTopic = env("LATE_TRANSACTIONS_TOPIC", "late-transactions");

        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();
        execution.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        execution.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
        execution.getCheckpointConfig().setCheckpointTimeout(60_000L);
        execution.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        execution.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        execution.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        execution.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(5)));

        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(transactionsTopic)
                .setGroupId("flink-fraud-detector")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setValueOnlyDeserializer(new TransactionDeserializationSchema())
                .build();

        WatermarkStrategy<Transaction> watermarks = WatermarkStrategy
                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((transaction, ignored) -> transaction.timestamp)
                .withIdleness(Duration.ofSeconds(15));

        SingleOutputStreamOperator<FraudAlert> alerts = execution
                .fromSource(source, watermarks, "Kafka transactions")
                .uid("transaction-source")
                .filter(FraudDetectionJob::isValid)
                .name("Validate transactions")
                .keyBy(transaction -> transaction.accountId)
                .process(new FraudDetector(RuleConfig.fromEnvironment()))
                .name("Stateful fraud rules")
                .uid("fraud-detector");

        DataStream<LateTransaction> lateTransactions = alerts.getSideOutput(FraudDetector.LATE_TRANSACTIONS);

        Properties producerProperties = new Properties();
        producerProperties.setProperty("transaction.timeout.ms", "300000");

        KafkaSink<FraudAlert> sink = KafkaSink.<FraudAlert>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAlert>builder()
                        .setTopic(alertsTopic)
                        .setValueSerializationSchema(new FraudAlertSerializationSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(env("ALERTS_TRANSACTIONAL_ID_PREFIX", "fraud-alerts-"))
                .setKafkaProducerConfig(producerProperties)
                .build();

        KafkaSink<LateTransaction> lateSink = KafkaSink.<LateTransaction>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<LateTransaction>builder()
                        .setTopic(lateTransactionsTopic)
                        .setValueSerializationSchema(new LateTransactionSerializationSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(env("LATE_TRANSACTIONAL_ID_PREFIX", "late-transactions-"))
                .setKafkaProducerConfig(producerProperties)
                .build();

        alerts.sinkTo(sink).name("Kafka fraud alerts").uid("alert-sink");
        lateTransactions.sinkTo(lateSink).name("Kafka late transactions").uid("late-transaction-sink");
        execution.execute("Realtime Financial Fraud Detection");
    }

    private static boolean isValid(Transaction transaction) {
        return transaction != null
                && transaction.transactionId != null
                && transaction.accountId != null
                && transaction.timestamp > 0
                && transaction.amount >= 0;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
