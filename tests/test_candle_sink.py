"""Tests for the candle sink service logic (no broker, MinIO or ClickHouse required)."""

from pathlib import Path

import storage.candle_sink as candle_sink
from ingestion import config

VALID_CANDLE = {
    "event_id": "NVDA20260625132800",
    "symbol": "NVDA",
    "interval": "1m",
    "timestamp": "2026-06-25T13:28:00+00:00",
    "open": 195.57,
    "high": 195.58,
    "low": 195.34,
    "close": 195.37,
    "volume": 162257.0,
    "source": config.SOURCE_YAHOO,
    "created_at": "2026-06-25T13:28:05+00:00",
}


class FakeProducer:
    """Captures error routing without touching Kafka."""

    def __init__(self):
        self.errors = []

    def send(self, topic, value=None, key=None):
        self.errors.append(topic)

    def close(self):
        pass


def _capture_sink(monkeypatch):
    """Replace sink_candles with a recorder; return the list it writes into."""
    written = []

    def fake_sink_candles(records, source="raw"):
        written.append((source, list(records)))

    monkeypatch.setattr(candle_sink, "sink_candles", fake_sink_candles)
    return written


class TestRetry:
    def test_retries_until_broker_available(self):
        attempts = []

        def flaky_factory():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("broker not ready")
            return "consumer"

        sleeps = []
        consumer = candle_sink._create_consumer_with_retry(
            consumer_factory=flaky_factory, initial_delay=1.0, max_delay=10.0, sleep=sleeps.append
        )

        assert consumer == "consumer"
        assert len(attempts) == 3
        assert sleeps == [1.0, 2.0]


class TestTopicRouting:
    def test_raw_batch_written_as_yahoo_finance(self, monkeypatch):
        written = _capture_sink(monkeypatch)

        count, rejected = candle_sink.sink_candle_batch(config.TOPIC_CANDLES_RAW, [VALID_CANDLE])

        assert (count, rejected) == (1, 0)
        assert written == [(config.SOURCE_YAHOO, [VALID_CANDLE])]

    def test_calculated_batch_written_as_spark_streaming(self, monkeypatch):
        written = _capture_sink(monkeypatch)
        spark_candle = {**VALID_CANDLE, "source": config.SOURCE_SPARK}

        count, rejected = candle_sink.sink_candle_batch(
            config.TOPIC_CANDLES_CALCULATED, [spark_candle]
        )

        assert (count, rejected) == (1, 0)
        assert written == [(config.SOURCE_SPARK, [spark_candle])]

    def test_raw_and_calculated_batches_are_not_mixed(self, monkeypatch):
        """A mixed batch would write Spark candles under one source, landing them in raw/."""
        written = _capture_sink(monkeypatch)
        spark_candle = {**VALID_CANDLE, "source": config.SOURCE_SPARK}

        candle_sink.sink_candle_batch(config.TOPIC_CANDLES_RAW, [VALID_CANDLE])
        candle_sink.sink_candle_batch(config.TOPIC_CANDLES_CALCULATED, [spark_candle])

        assert [source for source, _ in written] == [config.SOURCE_YAHOO, config.SOURCE_SPARK]


class TestValidation:
    def test_invalid_candle_routed_to_errors_not_storage(self, monkeypatch):
        written = _capture_sink(monkeypatch)
        producer = FakeProducer()
        # interval "2m" is not in the schema enum
        bad = {**VALID_CANDLE, "interval": "2m"}

        count, rejected = candle_sink.sink_candle_batch(config.TOPIC_CANDLES_RAW, [bad], producer)

        assert (count, rejected) == (0, 1)
        assert written == []
        assert producer.errors == [config.TOPIC_ERRORS]

    def test_valid_batch_emits_no_error(self, monkeypatch):
        _capture_sink(monkeypatch)
        producer = FakeProducer()

        candle_sink.sink_candle_batch(config.TOPIC_CANDLES_RAW, [VALID_CANDLE], producer)

        assert producer.errors == []

    def test_valid_records_survive_alongside_invalid(self, monkeypatch):
        written = _capture_sink(monkeypatch)
        producer = FakeProducer()
        bad = {**VALID_CANDLE, "interval": "2m"}

        count, rejected = candle_sink.sink_candle_batch(
            config.TOPIC_CANDLES_RAW, [bad, VALID_CANDLE], producer
        )

        assert (count, rejected) == (1, 1)
        assert written == [(config.SOURCE_YAHOO, [VALID_CANDLE])]


def test_sink_prefixes_are_distinct():
    """Calculated candles must not be written under the raw prefix."""
    from storage.sinks import _PREFIX_BY_SOURCE

    prefixes = {
        config.SOURCE_YAHOO: "raw/candles",
        config.SOURCE_SPARK: _PREFIX_BY_SOURCE[config.SOURCE_SPARK],
        config.SOURCE_RECONCILED: _PREFIX_BY_SOURCE[config.SOURCE_RECONCILED],
    }
    assert len(set(prefixes.values())) == 3
    assert prefixes[config.SOURCE_SPARK] != "raw/candles"


def test_candle_sink_service_mounts_source():
    """Guards the stale-image regression: a baked image silently ran code predating
    CLICKHOUSE_USER, so tick-sink failed every insert while still reporting healthy."""
    import yaml

    compose_path = Path(__file__).resolve().parent.parent / "docker-compose.yml"
    with open(compose_path) as f:
        services = yaml.safe_load(f)["services"]

    volumes = services["candle-sink"]["volumes"]
    for source_dir in ("./storage", "./entrypoint", "./ingestion"):
        assert any(v.startswith(source_dir + ":") for v in volumes), (
            f"candle-sink must mount {source_dir} so code edits need only a restart"
        )
