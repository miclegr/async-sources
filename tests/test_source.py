from asyncio import sleep
from async_sources.source import BatchedSource, Source
from async_sources.subscription import Subscription, SubscriptionItem
from .fixtures import *

def test_is_abstract():

    with pytest.raises(Exception):
        source = Source()

@pytest.mark.asyncio
async def test_source_creates_subscriptions(empty_source_class):

    inner_source = empty_source_class()
    source = empty_source_class(inner_source)

    assert source._sources == (inner_source,)
    assert len(source._source_subscriptions) == 1
    assert source._source_subscriptions[0]._source is inner_source 

@pytest.mark.asyncio
async def test_get_source_idx(empty_source_class):

    inner_source = empty_source_class()
    source = empty_source_class(inner_source)

    assert source._get_source_idx_from_subscription_id(None) is None
    assert source._get_source_idx_from_subscription_id(inner_source._subscriptions[0].id) == 0


@pytest.mark.asyncio
async def test_output_is_emitted(one_emitter_class):

    inner_source = one_emitter_class()
    subscription = inner_source.subscribe()
    got = await subscription.get()

    assert got.data == 1

@pytest.mark.asyncio
async def test_output_not_emitted(even_class):

    source = even_class()
    subscription = source.subscribe()

    got = await subscription.get()
    assert got.data == 0
    got = await subscription.get()
    assert got.data == 2
    

@pytest.mark.asyncio
async def test_chaining(one_emitter_class, add_class):
    
    source = add_class(one_emitter_class())
    subscription = source.subscribe()

    assert (await subscription.get()).data == 1
    assert (await subscription.get()).data == 2

@pytest.mark.asyncio
async def test_start_stop(empty_source_class):

    starting_n_tasks = len(asyncio.all_tasks())
    source = empty_source_class()
    n_tasks = len(asyncio.all_tasks())
    assert n_tasks == starting_n_tasks

    source.subscribe()
    n_tasks = len(asyncio.all_tasks())
    assert n_tasks == starting_n_tasks + 1

    starting_n_tasks = len(asyncio.all_tasks())
    source = empty_source_class(feeding_subscriptions_policy='never')
    n_tasks = len(asyncio.all_tasks())
    assert n_tasks == starting_n_tasks
    
    source.start_feeding_subscriptions()
    n_tasks = len(asyncio.all_tasks())
    assert n_tasks == starting_n_tasks + 1

    await source.stop_feeding_subscriptions()
    n_tasks = len(asyncio.all_tasks())
    assert n_tasks == starting_n_tasks

@pytest.mark.asyncio
async def test_batched_source(emit_elements_class):

    class TestBatchedSource(BatchedSource):
        async def _process_update(self, *args) -> List[Any]:
            [(data,_)] = args
            return [data]

    data = [1,2,3,4,5]

    emitter = emit_elements_class(data)
    batched_data = TestBatchedSource(emitter)

    await sleep(.1)
    emitted_data = (await batched_data.subscribe().get()).data
    assert data == emitted_data
