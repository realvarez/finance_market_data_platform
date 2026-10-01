from pathlib import Path
import yaml


def test_compose_kafka_and_ui_config():
    compose_path = (
        Path(__file__).resolve().parent.parent / "docker-compose.yml"
    )
    with open(compose_path) as f:
        config = yaml.safe_load(f)

    services = config["services"]

    # Broker assertions
    broker = services["broker"]
    assert broker["image"] == "confluentinc/cp-kafka:7.6.0"
    assert "KAFKA_HEAP_OPTS" in broker["environment"]
    assert broker["environment"]["KAFKA_HEAP_OPTS"] == "-Xms256m -Xmx512m"
    assert broker["mem_limit"] == "768m"

    # Schema Registry assertions (should be removed from default services)
    assert "schema-registry" not in services or "debug" in services.get(
        "schema-registry", {}
    ).get("profiles", [])

    # Kafka Init assertions
    kafka_init = services["kafka-init"]
    assert kafka_init["image"] == "confluentinc/cp-kafka:7.6.0"
    assert kafka_init["mem_limit"] == "128m"

    # UI assertions (AKHQ under profile 'ui')
    assert "control-center" not in services
    assert "akhq" in services
    assert "ui" in services["akhq"].get("profiles", [])
    assert services["akhq"]["mem_limit"] == "128m"
