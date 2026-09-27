import csv
import io
import json
import logging
import math
import os
import sqlite3
import threading
import time
import uuid
from collections import Counter, OrderedDict, defaultdict, deque

from confluent_kafka import Consumer, KafkaError, Producer
from flask import Flask, Response, jsonify, render_template, request


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("fraud-dashboard")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TRANSACTIONS_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "fraud-alerts")
LATE_TRANSACTIONS_TOPIC = os.getenv("LATE_TRANSACTIONS_TOPIC", "late-transactions")
GENERATOR_CONTROL_TOPIC = os.getenv("GENERATOR_CONTROL_TOPIC", "generator-control")
GENERATOR_STATUS_TOPIC = os.getenv("GENERATOR_STATUS_TOPIC", "generator-status")
CASE_DB_PATH = os.getenv("CASE_DB_PATH", "/tmp/fraud-cases.db")
CASE_STATUSES = {"NEW", "INVESTIGATING", "CONFIRMED_FRAUD", "FALSE_POSITIVE"}
RISK_WINDOW_OPTIONS = {0, 60, 300, 900, 3_600, 21_600, 86_400}


class CaseRepository:
    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_cases (
                    alert_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    assignee TEXT NOT NULL,
                    notes TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                )
                """
            )

    def _connect(self):
        return sqlite3.connect(self.path, timeout=5)

    def get(self, alert_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT status, assignee, notes, updated_at FROM alert_cases WHERE alert_id = ?",
                (alert_id,),
            ).fetchone()
        if row is None:
            return {"status": "NEW", "assignee": "", "notes": "", "updatedAt": None}
        return {"status": row[0], "assignee": row[1], "notes": row[2], "updatedAt": row[3]}

    def save(self, alert_id, status, assignee, notes):
        updated_at = int(time.time() * 1000)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO alert_cases(alert_id, status, assignee, notes, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(alert_id) DO UPDATE SET
                    status = excluded.status,
                    assignee = excluded.assignee,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (alert_id, status, assignee, notes, updated_at),
            )
        return {"status": status, "assignee": assignee, "notes": notes, "updatedAt": updated_at}


class DashboardStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.transactions = deque(maxlen=50)
        self.transactions_by_account = defaultdict(lambda: deque(maxlen=40))
        self.transactions_by_id = OrderedDict()
        self.alerts = deque(maxlen=1_000)
        self.alerts_by_id = OrderedDict()
        self.alerts_by_transaction_id = OrderedDict()
        self.late_transactions = deque(maxlen=30)
        self.seen_late_transaction_ids = set()
        self.recent_transaction_times = deque()
        self.total_transactions = 0
        self.injected_transactions = 0
        self.total_alerts = 0
        self.total_late_transactions = 0
        self.severities = Counter()
        self.reason_counts = Counter()
        self.seen_alert_ids = {}
        self.kafka_connected = False
        self.generator_status = None
        self.generator_status_received_at = 0

    def add_transaction(self, transaction):
        now = time.time()
        try:
            event_time = float(transaction.get("timestamp", now * 1000)) / 1000
        except (TypeError, ValueError):
            event_time = now

        with self.lock:
            self.total_transactions += 1
            if transaction.get("scenario", "NORMAL") != "NORMAL":
                self.injected_transactions += 1
            self.transactions.appendleft(transaction)
            account_id = transaction.get("accountId")
            if account_id:
                self.transactions_by_account[account_id].appendleft(transaction)
            transaction_id = transaction.get("transactionId")
            if transaction_id:
                self.transactions_by_id[transaction_id] = transaction
                self.transactions_by_id.move_to_end(transaction_id)
                while len(self.transactions_by_id) > 5_000:
                    self.transactions_by_id.popitem(last=False)
            # Kafka is replayed from the beginning whenever this demo dashboard
            # starts. Only events that really happened in the last minute belong
            # in the live throughput metric.
            if now - 60 <= event_time <= now + 5:
                self.recent_transaction_times.append(event_time)
            self._trim_rate_window(now)

    def add_alert(self, alert):
        alert_id = alert.get("alertId")
        with self.lock:
            if alert_id and alert_id in self.seen_alert_ids:
                return
            if alert_id:
                self.seen_alert_ids[alert_id] = time.time()
                if len(self.seen_alert_ids) > 50_000:
                    oldest = min(self.seen_alert_ids, key=self.seen_alert_ids.get)
                    self.seen_alert_ids.pop(oldest, None)
            self.total_alerts += 1
            self.alerts.appendleft(alert)
            if alert_id:
                self.alerts_by_id[alert_id] = alert
                self.alerts_by_id.move_to_end(alert_id)
                while len(self.alerts_by_id) > 50_000:
                    self.alerts_by_id.popitem(last=False)
            transaction_id = alert.get("transactionId")
            if transaction_id:
                self.alerts_by_transaction_id[transaction_id] = alert
                self.alerts_by_transaction_id.move_to_end(transaction_id)
                while len(self.alerts_by_transaction_id) > 5_000:
                    self.alerts_by_transaction_id.popitem(last=False)
            self.severities[alert.get("severity", "UNKNOWN")] += 1
            self.reason_counts.update(alert.get("reasons", []))

    def set_generator_status(self, status):
        with self.lock:
            self.generator_status = status
            self.generator_status_received_at = time.time()

    def add_late_transaction(self, late_transaction):
        transaction_id = late_transaction.get("transactionId")
        with self.lock:
            if transaction_id and transaction_id in self.seen_late_transaction_ids:
                return
            if transaction_id:
                self.seen_late_transaction_ids.add(transaction_id)
                if len(self.seen_late_transaction_ids) > 5_000:
                    self.seen_late_transaction_ids = {
                        item.get("transactionId")
                        for item in self.late_transactions
                        if item.get("transactionId")
                    }
            self.total_late_transactions += 1
            self.late_transactions.appendleft(late_transaction)

    def alert_detail(self, alert_id):
        with self.lock:
            alert = self.alerts_by_id.get(alert_id)
            if alert is None:
                return None
            account_id = alert.get("accountId")
            timeline = sorted(
                (dict(item) for item in self.transactions_by_account.get(account_id, [])),
                key=lambda item: item.get("timestamp", 0),
                reverse=True,
            )[:12]
            transaction = self.transactions_by_id.get(alert.get("transactionId"))
            return {
                "alert": dict(alert),
                "transaction": dict(transaction) if transaction else None,
                "timeline": timeline,
            }

    def query_events(self, filters):
        with self.lock:
            transactions = [dict(item) for item in self.transactions_by_id.values()]
            alerts = {key: dict(value) for key, value in self.alerts_by_transaction_id.items()}

        cutoff = None
        if filters["minutes"]:
            cutoff = int((time.time() - filters["minutes"] * 60) * 1000)

        records = []
        for transaction in transactions:
            alert = alerts.get(transaction.get("transactionId"))
            transaction_id = transaction.get("transactionId", "")
            account_id = transaction.get("accountId", "")
            severity = alert.get("severity") if alert else None

            if filters["query"] and filters["query"] not in transaction_id.lower():
                continue
            if filters["account"] and filters["account"] not in account_id.lower():
                continue
            if filters["country"] and transaction.get("country") != filters["country"]:
                continue
            if filters["scenario"] and transaction.get("scenario", "NORMAL") != filters["scenario"]:
                continue
            if filters["severity"] == "NONE" and alert:
                continue
            if filters["severity"] and filters["severity"] != "NONE" and severity != filters["severity"]:
                continue
            if cutoff and (transaction.get("timestamp") or 0) < cutoff:
                continue

            records.append(
                {
                    "transactionId": transaction_id,
                    "accountId": account_id,
                    "amount": transaction.get("amount", 0),
                    "currency": transaction.get("currency", "VND"),
                    "merchantCategory": transaction.get("merchantCategory"),
                    "country": transaction.get("country"),
                    "deviceId": transaction.get("deviceId"),
                    "timestamp": transaction.get("timestamp"),
                    "scenario": transaction.get("scenario", "NORMAL"),
                    "alertId": alert.get("alertId") if alert else None,
                    "severity": severity,
                    "riskScore": alert.get("riskScore", 0) if alert else 0,
                    "reasons": alert.get("reasons", []) if alert else [],
                }
            )

        sort_field = {"time": "timestamp", "amount": "amount", "risk": "riskScore"}[filters["sort"]]
        records.sort(
            key=lambda item: (item.get(sort_field) or 0, item.get("timestamp") or 0),
            reverse=filters["order"] == "desc",
        )
        return {"total": len(records), "items": records[: filters["limit"]]}

    def snapshot(self, risk_window_seconds=60):
        now = time.time()
        with self.lock:
            self._trim_rate_window(now)
            all_alerts = list(self.alerts_by_id.values())
            if risk_window_seconds:
                risk_window_start = int((now - risk_window_seconds) * 1000)
                recent_alerts = [
                    alert
                    for alert in all_alerts
                    if (alert.get("detectedAt") or 0) >= risk_window_start
                ]
            else:
                recent_alerts = all_alerts
            recent_severities = Counter(
                alert.get("severity", "UNKNOWN") for alert in recent_alerts
            )
            recent_reasons = Counter()
            for alert in recent_alerts:
                recent_reasons.update(alert.get("reasons", []))
            detection_rate = (
                round(self.total_alerts * 100 / self.total_transactions, 2)
                if self.total_transactions
                else 0
            )
            return {
                "connected": self.kafka_connected,
                "generator": {
                    **(self.generator_status or {
                        "paused": False,
                        "tps": 0,
                        "fraudIntervalSeconds": 0,
                        "generatedTotal": 0,
                        "lastScenario": None,
                        "lastScenarioAt": None,
                        "timestamp": None,
                    }),
                    "online": bool(self.generator_status and now - self.generator_status_received_at < 4),
                },
                "stats": {
                    "totalTransactions": self.total_transactions,
                    "injectedTransactions": self.injected_transactions,
                    "totalAlerts": self.total_alerts,
                    "detectionRate": detection_rate,
                    "transactionsPerMinute": len(self.recent_transaction_times),
                    "lateTransactions": self.total_late_transactions,
                },
                "riskWindowSeconds": risk_window_seconds,
                "riskAlertCount": len(recent_alerts),
                "severities": dict(recent_severities),
                "topReasons": [
                    {"reason": reason, "count": count}
                    for reason, count in recent_reasons.most_common(4)
                ],
                "transactions": list(self.transactions)[:20],
                "alerts": list(self.alerts)[:30],
                "lateTransactions": list(self.late_transactions)[:10],
                "serverTime": int(now * 1000),
            }

    def _trim_rate_window(self, now):
        self.recent_transaction_times = deque(
            timestamp
            for timestamp in self.recent_transaction_times
            if timestamp >= now - 60
        )


store = DashboardStore()
case_repository = CaseRepository(CASE_DB_PATH)
control_producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "fraud-dashboard-control",
        "acks": "all",
        "enable.idempotence": True,
    }
)
control_lock = threading.Lock()
app = Flask(__name__)


def consume_events():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"fraud-dashboard-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([
        TRANSACTIONS_TOPIC,
        ALERTS_TOPIC,
        LATE_TRANSACTIONS_TOPIC,
        GENERATOR_STATUS_TOPIC,
    ])
    LOG.info(
        "Dashboard đang đọc topics %s, %s, %s, %s",
        TRANSACTIONS_TOPIC,
        ALERTS_TOPIC,
        LATE_TRANSACTIONS_TOPIC,
        GENERATOR_STATUS_TOPIC,
    )

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    LOG.warning("Kafka consumer: %s", message.error())
                continue

            try:
                payload = json.loads(message.value().decode("utf-8"))
                with store.lock:
                    store.kafka_connected = True
                if message.topic() == TRANSACTIONS_TOPIC:
                    store.add_transaction(payload)
                elif message.topic() == ALERTS_TOPIC:
                    store.add_alert(payload)
                elif message.topic() == LATE_TRANSACTIONS_TOPIC:
                    store.add_late_transaction(payload)
                elif message.topic() == GENERATOR_STATUS_TOPIC:
                    store.set_generator_status(payload)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                LOG.warning("Bỏ qua message không hợp lệ: %s", error)
    finally:
        consumer.close()


consumer_thread = threading.Thread(target=consume_events, name="kafka-consumer", daemon=True)
consumer_thread.start()


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/snapshot")
def snapshot():
    try:
        risk_window_seconds = int(request.args.get("riskWindowSeconds", "60"))
    except ValueError:
        return jsonify({"error": "Mốc thời gian thống kê không hợp lệ"}), 400
    if risk_window_seconds not in RISK_WINDOW_OPTIONS:
        return jsonify({"error": "Mốc thời gian thống kê không được hỗ trợ"}), 400
    return jsonify(store.snapshot(risk_window_seconds))


def parse_event_filters(max_limit=500):
    severity = request.args.get("severity", "").upper()
    scenario = request.args.get("scenario", "").upper()
    sort = request.args.get("sort", "time").lower()
    order = request.args.get("order", "desc").lower()
    if severity not in {"", "NONE", "MEDIUM", "HIGH", "CRITICAL"}:
        raise ValueError("Mức độ cảnh báo không hợp lệ")
    if scenario not in {"", "NORMAL", "HIGH_AMOUNT", "RAPID_FIRE", "COUNTRY_HOP", "IMPOSSIBLE_TRAVEL", "OUT_OF_ORDER"}:
        raise ValueError("Kịch bản không hợp lệ")
    if sort not in {"time", "amount", "risk"} or order not in {"asc", "desc"}:
        raise ValueError("Cách sắp xếp không hợp lệ")

    try:
        minutes = int(request.args.get("minutes", "0"))
        limit = int(request.args.get("limit", "100"))
    except ValueError as error:
        raise ValueError("Khoảng thời gian hoặc giới hạn không hợp lệ") from error
    if minutes < 0 or minutes > 10_080:
        raise ValueError("Khoảng thời gian phải nằm trong 7 ngày")
    if limit < 1 or limit > max_limit:
        raise ValueError(f"Giới hạn phải nằm trong khoảng 1–{max_limit}")

    query = request.args.get("q", "").strip().lower()
    account = request.args.get("account", "").strip().lower()
    country = request.args.get("country", "").strip().upper()
    if len(query) > 100 or len(account) > 80 or len(country) > 3:
        raise ValueError("Giá trị tìm kiếm quá dài")
    return {
        "query": query,
        "account": account,
        "country": country,
        "scenario": scenario,
        "severity": severity,
        "minutes": minutes,
        "sort": sort,
        "order": order,
        "limit": limit,
    }


@app.get("/api/events")
def query_events():
    try:
        filters = parse_event_filters()
    except ValueError as error:
        return jsonify({"error": str(error)}), 400
    return jsonify(store.query_events(filters))


@app.get("/api/events/export")
def export_events():
    export_format = request.args.get("format", "csv").lower()
    if export_format not in {"csv", "json"}:
        return jsonify({"error": "Định dạng xuất phải là csv hoặc json"}), 400
    try:
        filters = parse_event_filters(max_limit=5_000)
    except ValueError as error:
        return jsonify({"error": str(error)}), 400
    filters["limit"] = 5_000
    result = store.query_events(filters)
    timestamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())

    if export_format == "json":
        content = json.dumps(result["items"], ensure_ascii=False, indent=2)
        return Response(
            content,
            content_type="application/json; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="fraud-events-{timestamp}.json"'},
        )

    output = io.StringIO()
    output.write("\ufeff")
    fields = [
        "transactionId", "accountId", "amount", "currency", "merchantCategory",
        "country", "deviceId", "timestamp", "scenario", "alertId", "severity",
        "riskScore", "reasons",
    ]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for item in result["items"]:
        row = dict(item)
        row["reasons"] = " | ".join(row["reasons"])
        writer.writerow(row)
    return Response(
        output.getvalue(),
        content_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="fraud-events-{timestamp}.csv"'},
    )


@app.get("/api/alerts/<alert_id>")
def alert_detail(alert_id):
    detail = store.alert_detail(alert_id)
    if detail is None:
        return jsonify({"error": "Không tìm thấy cảnh báo"}), 404
    detail["case"] = case_repository.get(alert_id)
    return jsonify(detail)


@app.patch("/api/alerts/<alert_id>/case")
def update_alert_case(alert_id):
    if store.alert_detail(alert_id) is None:
        return jsonify({"error": "Không tìm thấy cảnh báo"}), 404

    payload = request.get_json(silent=True) or {}
    current = case_repository.get(alert_id)
    status = payload.get("status", current["status"])
    assignee = str(payload.get("assignee", current["assignee"])).strip()
    notes = str(payload.get("notes", current["notes"])).strip()
    if status not in CASE_STATUSES:
        return jsonify({"error": "Trạng thái hồ sơ không hợp lệ"}), 400
    if len(assignee) > 80 or len(notes) > 2_000:
        return jsonify({"error": "Người xử lý hoặc ghi chú vượt quá độ dài cho phép"}), 400
    return jsonify({"case": case_repository.save(alert_id, status, assignee, notes)})


@app.post("/api/generator/control")
def control_generator():
    payload = request.get_json(silent=True) or {}
    command = payload.get("command")
    message = {"command": command, "requestedAt": int(time.time() * 1000)}

    if command == "configure":
        if "paused" not in payload and "tps" not in payload:
            return jsonify({"error": "Cần truyền paused hoặc tps"}), 400
        if "paused" in payload:
            if not isinstance(payload["paused"], bool):
                return jsonify({"error": "paused phải là boolean"}), 400
            message["paused"] = payload["paused"]
        if "tps" in payload:
            value = payload["tps"]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                return jsonify({"error": "TPS phải là một số"}), 400
            if not 0.2 <= value <= 100:
                return jsonify({"error": "TPS phải nằm trong khoảng 0.2–100"}), 400
            message["tps"] = round(float(value), 1)
    elif command == "inject":
        scenario = payload.get("scenario")
        if scenario not in {"HIGH_AMOUNT", "RAPID_FIRE", "COUNTRY_HOP", "IMPOSSIBLE_TRAVEL", "OUT_OF_ORDER"}:
            return jsonify({"error": "Kịch bản không hợp lệ"}), 400
        message["scenario"] = scenario
    else:
        return jsonify({"error": "Lệnh generator không hợp lệ"}), 400

    encoded = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    try:
        with control_lock:
            control_producer.produce(GENERATOR_CONTROL_TOPIC, key=b"generator", value=encoded)
            remaining = control_producer.flush(3)
        if remaining:
            raise RuntimeError("Kafka chưa xác nhận lệnh")
    except Exception as error:
        LOG.exception("Không gửi được lệnh generator")
        return jsonify({"error": str(error)}), 503
    return jsonify({"queued": True, "command": message}), 202


@app.get("/health")
def health():
    return jsonify({"status": "ok", "consumerAlive": consumer_thread.is_alive()})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
