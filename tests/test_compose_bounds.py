from pathlib import Path

import yaml


def test_compose_storage_and_airflow_bounds():
    compose_path = Path(__file__).resolve().parent.parent / "docker-compose.yml"
    with open(compose_path) as f:
        config = yaml.safe_load(f)

    services = config["services"]

    # MinIO
    minio = services["minio"]
    assert minio["mem_limit"] == "256m"
    assert minio["environment"]["MINIO_API_REQUESTS_MAX"] == "100"

    # ClickHouse
    ch = services["clickhouse"]
    assert ch["mem_limit"] == "1280m"
    assert any("memory.xml" in v for v in ch["volumes"])

    # Tick Ingestion
    tick = services["tick-ingestion"]
    assert tick["mem_limit"] == "192m"

    # Tick Sink
    sink = services["tick-sink"]
    assert sink["mem_limit"] == "192m"
    assert sink["cpus"] == 0.5
    assert sink["depends_on"]["broker"]["condition"] == "service_healthy"
    assert sink["depends_on"]["minio"]["condition"] == "service_healthy"
    assert sink["depends_on"]["minio-init"]["condition"] == "service_completed_successfully"
    assert sink["depends_on"]["clickhouse"]["condition"] == "service_healthy"
    assert sink["environment"]["MINIO_ENDPOINT"] == "minio:9000"

    # Postgres
    pg = services["postgres"]
    assert pg["mem_limit"] == "192m"
    assert "shared_buffers=64MB" in pg["command"]

    # Airflow
    af = services["airflow"]
    assert af["mem_limit"] == "1024m"
    env = af["environment"]
    assert env["AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL"] == "60"
    assert env["AIRFLOW__SCHEDULER__PARSING_PROCESSES"] == "1"
    assert env["AIRFLOW__CORE__PARALLELISM"] == "4"
