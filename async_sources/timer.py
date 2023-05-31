from .source import Source
from asyncio import sleep
from datetime import datetime, timezone
from typing import List

class TimerSource(Source):

    def __init__(self, period_seconds: float, timezone = timezone.utc, feeding_subscriptions_policy: str = 'on_subscribe'):
        self.period_seconds = period_seconds
        self.timezone = timezone
        super().__init__(feeding_subscriptions_policy=feeding_subscriptions_policy)

    async def _process_update(self, *args) -> List[datetime]:

        await sleep(self.period_seconds)
        return [datetime.now(self.timezone)]

