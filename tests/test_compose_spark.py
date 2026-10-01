from pathlib import Path
import yaml


def test_compose_spark_consolidation():
    compose_path = (
        Path(__file__).resolve().parent.parent / "docker-compose.yml"
    )
    with open(compose_path) as f:
        config = yaml.safe_load(f)

    services = config["services"]

    # candle-builder is standalone and has correct master and memory
    candle_builder = services["candle-builder"]
    assert candle_builder["mem_limit"] == "1024m"
    assert "--master local[2]" in candle_builder["command"]
    assert "--driver-memory 768m" in candle_builder["command"]
    assert "spark.sql.shuffle.partitions=2" in candle_builder["command"]

    # depends_on must not depend on spark-worker
    depends_on = candle_builder["depends_on"]
    assert "spark-worker" not in depends_on
    assert "spark-master" not in depends_on
    assert "broker" in depends_on

    # spark-master and spark-worker should be gated behind 'spark-cluster' profile
    if "spark-master" in services:
        assert "spark-cluster" in services["spark-master"].get("profiles", [])
    if "spark-worker" in services:
        assert "spark-cluster" in services["spark-worker"].get("profiles", [])
