import pytest
from datetime import datetime, timedelta
from .fixtures import *
from async_sources.source import Source
from async_sources.interval import IntervalSource, Interval


@pytest.fixture
def interval_class():

    class TestIntervalSource(IntervalSource):

        @staticmethod
        def _key_by_time(data):
            return data

        @staticmethod
        def _is_interval_ready(interval):
            return len(interval) == 3

        def _update_active_intervals(self, intervals, key):
            if not any(interval.start_date == key for interval in intervals):
                return [Interval(key, key + timedelta(seconds=3), payload='hello')]

    return TestIntervalSource


def test_is_abstract():

    with pytest.raises(Exception):
        source = IntervalSource()

@pytest.mark.asyncio
async def test_overall(interval_class, clock_class):

    source = interval_class(clock_class())
    subscription = source.subscribe()
    item = await subscription.get()

    assert len(item.data.store) == 3

@pytest.mark.asyncio
async def test_finalize(interval_class, clock_class):

    class TestFinalize(interval_class):

        def _finalize_interval(self, interval):
            return {interval.attributes['payload']:len(interval.store)}

    source = TestFinalize(clock_class())
    subscription = source.subscribe()
    item = await subscription.get()

    assert item.data == {'hello':3}
