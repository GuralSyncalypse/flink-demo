.PHONY: up down logs test savepoint clean

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f generator jobmanager taskmanager dashboard

test:
	docker build --target test -t fraud-detection-flink-test ./flink-job

savepoint:
	docker compose exec jobmanager /opt/flink/bin/flink savepoint $(JOB_ID) file:///flink-data/savepoints

clean:
	docker compose down -v --remove-orphans
