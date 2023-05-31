import asyncio
from datetime import datetime
from typing import List, Any
from async_sources.source import NoUpdate, Source
import pytest

@pytest.fixture
def empty_source_class():

    class EmptySource(Source):

        async def _process_update(self, *args):
            raise NoUpdate

    return EmptySource

@pytest.fixture
def one_emitter_class():

    class OneEmitter(Source):

        async def _process_update(self, *args):
            await asyncio.sleep(0.01)
            return [1.]

    return OneEmitter


@pytest.fixture
def add_class():

    class Add(Source):

        def _setup_internal_store(self):
            self.count = 0

        async def _process_update(self, *args):

            update, _ = args[0]
            self.count += update
            return [self.count]

    return Add

@pytest.fixture
def numbers_class():

    class Numbers(Source):

        def _setup_internal_store(self):

            def numbers():
                i = 0
                while True:
                    yield i
                    i+=1

            self.store = numbers()

        async def _process_update(self, *args):

            update = next(self.store)
            return [update]

    return Numbers

@pytest.fixture
def even_class(numbers_class):

    class Even(numbers_class):

        async def _process_update(self, *args):

            update = next(self.store)
            if update % 2 == 0:
                return [update]
            else:
                raise NoUpdate

    return Even

@pytest.fixture
def clock_class():

    class Clock(Source):

        async def _process_update(self, *args) -> List[datetime]:

            await asyncio.sleep(.1)
            return [datetime.now()]

    return Clock


@pytest.fixture
def emit_single_element_class():

    class EmitSingleElement(Source):

        def __init__(self, what):
            self.what = what
            self.init = True
            super().__init__()

        async def _process_update(self, *args) -> List[Any]:

            if self.init:
                return [self.what]
            else:
                raise NoUpdate

    return EmitSingleElement

@pytest.fixture
def emit_elements_class():

    class EmitElements(Source):

        def __init__(self, what):
            self.what = what[::-1]
            super().__init__()

        async def _process_update(self, *args) -> List[Any]:

            if len(self.what):
                return [self.what.pop()]
            else:
                raise NoUpdate

    return EmitElements
