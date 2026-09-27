import json
import logging
import math
import os
import random
import signal
import time
import uuid
from collections import deque
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("transaction-generator")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
CONTROL_TOPIC = os.getenv("GENERATOR_CONTROL_TOPIC", "generator-control")
STATUS_TOPIC = os.getenv("GENERATOR_STATUS_TOPIC", "generator-status")
TPS = max(0.2, float(os.getenv("TRANSACTIONS_PER_SECOND", "4")))
FRAUD_INTERVAL = max(2.0, float(os.getenv("FRAUD_INTERVAL_SECONDS", "8")))
SCENARIOS = ("HIGH_AMOUNT", "RAPID_FIRE", "COUNTRY_HOP", "IMPOSSIBLE_TRAVEL", "OUT_OF_ORDER")

COUNTRIES = ["VN", "SG", "TH", "JP"]
MERCHANTS = [
    ("MRC-GROCERY", "GROCERY"),
    ("MRC-ELECTRONICS", "ELECTRONICS"),
    ("MRC-TRAVEL", "TRAVEL"),
    ("MRC-FASHION", "FASHION"),
    ("MRC-DINING", "DINING"),
]
ACCOUNTS = [
    {
        "account_id": f"ACC-{index:04d}",
        "country": COUNTRIES[(index - 1) % len(COUNTRIES)],
        "device_id": f"device-{index:04d}",
    }
    for index in range(1001, 1501)
]

running = True


def stop(*_args):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def epoch_millis():
    return int(time.time() * 1000)


def transaction(account, scenario="NORMAL", **changes):
    merchant_id, category = random.choice(MERCHANTS)
    amount = int(min(30_000_000, max(20_000, random.lognormvariate(math.log(1_200_000), 0.9))))
    payload = {
        "transactionId": str(uuid.uuid4()),
        "accountId": account["account_id"],
        "amount": amount,
        "currency": "VND",
        "merchantId": merchant_id,
        "merchantCategory": category,
        "country": account["country"],
        "deviceId": account["device_id"],
        "timestamp": epoch_millis(),
        "scenario": scenario,
    }
    payload.update(changes)
    return payload


def fraud_scenario(sequence, requested=None):
    scenario = requested or SCENARIOS[sequence % len(SCENARIOS)]
    if scenario == "HIGH_AMOUNT":
        account = {"account_id": f"SIM-HIGH-{sequence}", "country": "VN", "device_id": "sim-high"}
        return [transaction(account, "HIGH_AMOUNT", amount=random.randint(120_000_000, 350_000_000))]

    if scenario == "RAPID_FIRE":
        account = {"account_id": f"SIM-BURST-{sequence}", "country": "VN", "device_id": "sim-burst"}
        return [transaction(account, "RAPID_FIRE", amount=random.randint(50_000, 900_000)) for _ in range(6)]

    if scenario == "OUT_OF_ORDER":
        account = {"account_id": f"SIM-LATE-{sequence}", "country": "VN", "device_id": "sim-late"}
        return [transaction(account, "OUT_OF_ORDER", timestamp=epoch_millis() - 30_000)]

    if scenario == "COUNTRY_HOP":
        account = {
            "account_id": f"SIM-HOP-{sequence}",
            "country": "VN",
            "device_id": "sim-hop-phone",
        }
        first = transaction(account, "COUNTRY_HOP", amount=random.randint(100_000, 2_000_000))
        second = transaction(
            account,
            "COUNTRY_HOP",
            amount=random.randint(100_000, 2_000_000),
            country="SG",
            timestamp=first["timestamp"] + 250,
        )
        return [first, second]

    account = {
        "account_id": f"SIM-TRAVEL-{sequence}",
        "country": "VN",
        "device_id": "sim-phone-vn",
    }
    first = transaction(account, "IMPOSSIBLE_TRAVEL")
    second = transaction(
        account,
        "IMPOSSIBLE_TRAVEL",
        amount=random.randint(120_000_000, 350_000_000),
        country="US",
        deviceId="sim-phone-us",
        timestamp=first["timestamp"] + 250,
    )
    return [first, second]


def delivery_report(error, message):
    if error:
        LOG.error("Không gửi được giao dịch tới Kafka: %s", error)


def publish_json(producer, topic, key, payload):
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    while running:
        try:
            producer.produce(
                topic,
                key=key.encode("utf-8"),
                value=encoded,
                on_delivery=delivery_report,
            )
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.25)


def publish_transaction(producer, payload):
    publish_json(producer, TOPIC, payload["accountId"], payload)


def publish_status(producer, state):
    publish_json(
        producer,
        STATUS_TOPIC,
        "generator",
        {
            "paused": state["paused"],
            "tps": state["tps"],
            "fraudIntervalSeconds": FRAUD_INTERVAL,
            "generatedTotal": state["generated_total"],
            "lastScenario": state["last_scenario"],
            "lastScenarioAt": state["last_scenario_at"],
            "timestamp": epoch_millis(),
        },
    )


def consume_control(consumer, state, pending_scenarios):
    changed = False
    while True:
        message = consumer.poll(0)
        if message is None:
            break
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                LOG.warning("Không đọc được lệnh điều khiển: %s", message.error())
            continue

        try:
            command = json.loads(message.value().decode("utf-8"))
            if command.get("command") == "configure":
                if isinstance(command.get("paused"), bool):
                    state["paused"] = command["paused"]
                if "tps" in command:
                    state["tps"] = min(100.0, max(0.2, float(command["tps"])))
                changed = True
                LOG.info("Cập nhật generator: paused=%s, tps=%.1f", state["paused"], state["tps"])
            elif command.get("command") == "inject" and command.get("scenario") in SCENARIOS:
                pending_scenarios.append(command["scenario"])
                changed = True
                LOG.info("Đã nhận yêu cầu phát kịch bản %s", command["scenario"])
            else:
                LOG.warning("Bỏ qua lệnh generator không hợp lệ: %s", command)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
            LOG.warning("Bỏ qua lệnh generator lỗi: %s", error)
    return changed


def main():
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "fraud-demo-generator",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "fraud-demo-generator-control",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([CONTROL_TOPIC])

    state = {
        "paused": False,
        "tps": TPS,
        "generated_total": 0,
        "last_scenario": None,
        "last_scenario_at": None,
    }
    pending_scenarios = deque()
    LOG.info("Bắt đầu mô phỏng %.1f giao dịch/giây; kịch bản gian lận mỗi %.1f giây", state["tps"], FRAUD_INTERVAL)

    next_normal = time.monotonic()
    next_fraud = time.monotonic() + FRAUD_INTERVAL
    next_status = 0.0
    fraud_sequence = 0

    try:
        while running:
            now = time.monotonic()
            if consume_control(consumer, state, pending_scenarios):
                next_status = 0.0
                next_normal = now
                next_fraud = now + FRAUD_INTERVAL

            requested_scenario = pending_scenarios.popleft() if pending_scenarios else None
            scheduled_scenario = not state["paused"] and now >= next_fraud
            if requested_scenario or scheduled_scenario:
                fraud_sequence += 1
                events = fraud_scenario(fraud_sequence, requested_scenario)
                state["last_scenario"] = events[0]["scenario"]
                state["last_scenario_at"] = epoch_millis()
                LOG.info("Phát kịch bản %s (%d giao dịch)", events[0]["scenario"], len(events))
                for event in events:
                    publish_transaction(producer, event)
                    state["generated_total"] += 1
                    time.sleep(0.05)
                if scheduled_scenario:
                    next_fraud = now + FRAUD_INTERVAL
                next_status = 0.0

            if not state["paused"] and now >= next_normal:
                publish_transaction(producer, transaction(random.choice(ACCOUNTS)))
                state["generated_total"] += 1
                next_normal = max(next_normal + (1.0 / state["tps"]), now)

            if now >= next_status:
                publish_status(producer, state)
                next_status = now + 1.0

            producer.poll(0)
            time.sleep(0.01)
    finally:
        consumer.close()
        LOG.info("Đang gửi nốt các giao dịch trong bộ đệm...")
        producer.flush(10)


if __name__ == "__main__":
    LOG.info("UTC start time: %s", datetime.now(timezone.utc).isoformat())
    main()
