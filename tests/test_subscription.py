from async_sources.source import Source
from async_sources.subscription import Subscription, SubscriptionItem
from .fixtures import *


@pytest.mark.asyncio
async def test_subscription(empty_source_class):

    inner_source = empty_source_class()
    subscription = Subscription(inner_source)
    payload = 'hello world'

    subscription.feed(payload)

    assert subscription.queue_size() == 1

    received = await subscription.get()

    expected = SubscriptionItem(subscription.id, payload)

    assert expected.subscription_id == received.subscription_id
    assert expected.data == received.data
