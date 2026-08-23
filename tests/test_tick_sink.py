"""Tests for the tick sink service logic (no broker required)."""

import storage.tick_sink as tick_sink


class TestConsumerRetry:
    def test_retries_until_broker_available(self):
        attempts = []

        def flaky_factory():
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("broker not ready")
            return "consumer"

        sleeps = []
        consumer = tick_sink._create_consumer_with_retry(
            consumer_factory=flaky_factory, initial_delay=1.0, max_delay=10.0, sleep=sleeps.append
        )

        assert consumer == "consumer"
        assert len(attempts) == 3
        # Exponential backoff: 1s then 2s before the successful attempt.
        assert sleeps == [1.0, 2.0]

    def test_backoff_capped_at_max_delay(self):
        attempts = []

        def flaky_factory():
            attempts.append(1)
            if len(attempts) < 5:
                raise ConnectionError("broker down")
            return "consumer"

        sleeps = []
        consumer = tick_sink._create_consumer_with_retry(
            consumer_factory=flaky_factory, initial_delay=20.0, max_delay=30.0, sleep=sleeps.append
        )

        assert consumer == "consumer"
        assert len(attempts) == 5
        # 20 -> 40 capped to 30, then stays at 30.
        assert sleeps == [20.0, 30.0, 30.0, 30.0]
