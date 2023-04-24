import asyncio
from async_sources.source import NoUpdate, Source
import pytest

@pytest.fixture
def empty_source_class():

    class EmptySource(Source):

        async def _process_update(self, *args):
            raise NoUpdate

    return EmptySource

@pytest.fixture
def passthrough_source_class():

    class Passthrough(Source):

        async def _process_update(self, *args):
            for arg in args:
                update, _ = arg

            return update

    return Passthrough

@pytest.fixture
def one_emitter_class():

    class OneEmitter(Source):

        async def _process_update(self, *args):
            await asyncio.sleep(0.01)
            return 1.

    return OneEmitter


@pytest.fixture
def add_class():

    class Add(Source):

        def _setup_internal_store(self):
            self.count = 0

        async def _process_update(self, *args):

            update, _ = args[0]
            self.count += update
            return self.count

    return Add

@pytest.fixture
def even_class():

    class Even(Source):

        def _setup_internal_store(self):

            def numbers():
                i = 0
                while True:
                    yield i
                    i+=1

            self.store = numbers()

        async def _process_update(self, *args):

            update = next(self.store)
            if update % 2 == 0:
                return update
            else:
                raise NoUpdate

    return Even
