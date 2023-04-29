from async_sources.concat import ConcatSource
import pytest
from .fixtures import *

@pytest.mark.asyncio
async def test_concat(emit_single_element_class):

    s1 = emit_single_element_class(1)
    s2 = emit_single_element_class(2)

    source = ConcatSource(s1,s2)
    subscription = source.subscribe()

    data =[]
    data.append((await subscription.get()).data)
    data.append((await subscription.get()).data)

    assert set(data) == {1,2}

