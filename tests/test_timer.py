from .fixtures import *
from async_sources.timer import TimerSource, ClockSource
from datetime import datetime
import pytest

@pytest.mark.asyncio
async def test_timer():

    initial_delay_seconds = 0.1
    period = 0.5
    source = TimerSource(initial_delay_seconds, period, feeding_subscriptions_policy='immediate')
    subscription = source.subscribe()

    await subscription.get()
    tick = datetime.now()
    await subscription.get()
    tock = datetime.now()

    assert abs((tock-tick).total_seconds() - period) < 0.1

@pytest.mark.asyncio
async def test_clock():

    period = 1
    source = ClockSource(period, feeding_subscriptions_policy='immediate')
    subscription = source.subscribe()

    tick=(await subscription.get()).data
    real_tick = datetime.now()
    tock =(await subscription.get()).data
    real_tock = datetime.now()

    assert abs((real_tock-real_tick).total_seconds() - period) < 0.1
    assert tick.microsecond == 0
    assert tock.microsecond == 0
    assert tick.tzinfo == source.timezone
    assert tock.tzinfo == source.timezone
