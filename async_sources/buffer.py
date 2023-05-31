from .source import Source, NoUpdate
from typing import List, Any

class BufferSource(Source):

    def __init__(self, source: Source, trigger: Source, feeding_subscriptions_policy: str = 'on_subscribe'):
        self.buffer = []
        super().__init__(source, trigger, feeding_subscriptions_policy=feeding_subscriptions_policy)

    async def _process_update(self, *args) -> List[Any]:

        args = sorted(args, key=lambda x: x[1])
        if len(args) == 1:
            data, idx = args[0]

            if idx == 0: # source
                self.buffer.append(data)
                raise NoUpdate
            else: #trigger

                output = self.buffer
                self.buffer = []
                return [output]

        elif len(args) == 2:

            data, _ = args[0]
            self.buffer.append(data)
            output = self.buffer
            self.buffer = []
            return [output]

        else:
            raise AttributeError

