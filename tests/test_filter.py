from .fixtures import numbers_class
from async_sources.filter import FilterSource
import pytest

def test_is_abstract():
    with pytest.raises(Exception):
        FilterSource()

@pytest.mark.asyncio
async def test_filter_source(numbers_class):

    numbers = numbers_class()

    class EvenFilter(FilterSource):

        def _filter_fn(self, value):
            return value % 2 == 0

    source = EvenFilter(numbers)
    subscription = source.subscribe()

    for _ in range(10):
        data = (await subscription.get()).data
        assert data % 2 == 0
