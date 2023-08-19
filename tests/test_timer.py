from .fixtures import *
from async_sources.timer import TimerSource
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
