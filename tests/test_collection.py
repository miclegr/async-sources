import asyncio
import pytest
from .fixtures import *
from async_sources.collection.base import CollectionSource
from async_sources.collection.fork import ForkSource
from async_sources.collection.map import MapCollection
from async_sources.source import Source

@pytest.fixture
def test_collection_class():

    class TestCollection(ForkSource):

        @staticmethod
        def _get_keys_from_update(data):
            return ['key']

    return TestCollection

@pytest.mark.asyncio
async def test_inner_fork_creation(test_collection_class, one_emitter_class):

    parent = one_emitter_class()
    source = test_collection_class(parent)

    await source.subscribe().get()
    assert source.get_inner_source('key').set

@pytest.mark.asyncio
async def test_collection_output(test_collection_class, one_emitter_class):

    parent = one_emitter_class()
    source = test_collection_class(parent)

    output = (await source.subscribe().get()).data
    assert output == 'key'

    subscription = source.subscribe_key('key')
    assert (await subscription.get()).data == 1


@pytest.mark.asyncio
async def test_map_fork(numbers_class, add_class):

    class PairOddFork(ForkSource):

        @staticmethod
        def _get_keys_from_update(data):
            assert isinstance(data, int)
            key = 'pair' if data % 2 == 0 else 'odd'
            return [key]

    class MapAdd(MapCollection):

        @staticmethod
        def _mapping_fn(source):
            return add_class(source)

    forked = PairOddFork(numbers_class())
    mapped = MapAdd(forked)

    subscription = mapped.subscribe_key('pair')
    out = (await subscription.get()).data
    assert out == 0
    out = (await subscription.get()).data
    assert out == 2 + 0
    out = (await subscription.get()).data
    assert out == 4 + 2 + 0

    

