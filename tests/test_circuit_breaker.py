import time

import pytest
from api_client import CircuitBreaker, CircuitState


@pytest.fixture
def breaker():
    return CircuitBreaker()


@pytest.mark.asyncio
async def test_initial_state_closed(breaker):
    """New breaker starts in CLOSED state and allows execution."""
    assert breaker.state == CircuitState.CLOSED
    assert await breaker.can_execute() is True


@pytest.mark.asyncio
async def test_stays_closed_below_threshold(breaker):
    """Record 2 failures (below threshold of 3) — still allows execution."""
    await breaker.record_failure()
    await breaker.record_failure()
    assert breaker.state == CircuitState.CLOSED
    assert await breaker.can_execute() is True


@pytest.mark.asyncio
async def test_opens_at_threshold(breaker):
    """Record 3 failures (reaches threshold) — circuit opens, blocks execution."""
    for _ in range(3):
        await breaker.record_failure()
    assert breaker.state == CircuitState.OPEN
    assert await breaker.can_execute() is False


@pytest.mark.asyncio
async def test_success_resets_failure_count(breaker):
    """Record 2 failures, 1 success, 2 more failures — still closed."""
    await breaker.record_failure()
    await breaker.record_failure()
    await breaker.record_success()

    # Failure count was reset by success, so 2 more shouldn't trip it
    await breaker.record_failure()
    await breaker.record_failure()

    assert breaker.state == CircuitState.CLOSED
    assert await breaker.can_execute() is True


@pytest.mark.asyncio
async def test_half_open_after_timeout(breaker):
    """Open the breaker, simulate timeout passing, verify transition to HALF_OPEN."""
    # Trip the breaker
    for _ in range(3):
        await breaker.record_failure()
    assert breaker.state == CircuitState.OPEN

    # Simulate 31 seconds passing by backdating last_failure_time
    breaker.last_failure_time = time.monotonic() - 31

    # can_execute should transition to HALF_OPEN and return True
    assert await breaker.can_execute() is True
    assert breaker.state == CircuitState.HALF_OPEN


@pytest.mark.asyncio
async def test_half_open_success_closes(breaker):
    """Get to HALF_OPEN state, record success — state becomes CLOSED."""
    # Trip the breaker and simulate timeout
    for _ in range(3):
        await breaker.record_failure()
    breaker.last_failure_time = time.monotonic() - 31
    await breaker.can_execute()  # Transitions to HALF_OPEN
    assert breaker.state == CircuitState.HALF_OPEN

    # Success in HALF_OPEN should close the circuit
    await breaker.record_success()
    assert breaker.state == CircuitState.CLOSED
    assert breaker.failure_count == 0


@pytest.mark.asyncio
async def test_half_open_failure_reopens(breaker):
    """Get to HALF_OPEN state, record failure — state returns to OPEN."""
    # Trip the breaker and simulate timeout
    for _ in range(3):
        await breaker.record_failure()
    breaker.last_failure_time = time.monotonic() - 31
    await breaker.can_execute()  # Transitions to HALF_OPEN
    assert breaker.state == CircuitState.HALF_OPEN

    # Failure in HALF_OPEN should reopen the circuit
    await breaker.record_failure()
    assert breaker.state == CircuitState.OPEN
