.PHONY: up down ready test smoke monitoring
up:
	docker compose up -d
ready:
	python -m lab.cli wait

down:
	docker compose down

test:
	python -m unittest discover -s tests -v

smoke:
	python -m lab.cli wait
	python -m lab.cli bootstrap
	python -m lab.cli ingest specs/orders_raw.json
	python -m lab.cli generate --count 1000 --late-every 5 --duplicate-every 10 --bad-records 3 --schema-v2 --output data/smoke.jsonl
	python -m lab.cli send data/smoke.jsonl
	python -m lab.cli verify data/smoke.jsonl.manifest.json --timeout 300

monitoring:
	docker compose --profile monitoring up -d prometheus

.PHONY: flink-up flink-submit flink-list
flink-up:
	docker compose --profile flink build flink-jobmanager
	docker compose --profile flink up -d flink-jobmanager flink-taskmanager

flink-submit:
	docker compose --profile flink --profile flink-tools run --rm flink-client flink run -d -m flink-jobmanager:8081 -py /opt/flink/usrlib/stream_job.py -pyfs /opt/flink/usrlib/stream_contract.py

flink-list:
	docker compose --profile flink --profile flink-tools run --rm flink-client flink list -m flink-jobmanager:8081
