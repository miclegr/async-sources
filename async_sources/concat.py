from typing import List, Any
from .source import Source

class ConcatSource(Source):

    async def _process_update(self, *args) -> List[Any]:

        out = []
        for arg in args:
            data, _ = arg
            out.append(data)
        return out
