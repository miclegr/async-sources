from .source import Source
from asyncio import sleep
from datetime import datetime, timezone
from typing import List

class TimerSource(Source):

    def __init__(self, initial_delay_seconds: float, period_seconds: float, timezone = timezone.utc, feeding_subscriptions_policy: str = 'on_subscribe'):
        self.period_seconds = period_seconds
        self.timezone = timezone
        self.initial_delay_seconds = initial_delay_seconds
        self.init = True
        super().__init__(feeding_subscriptions_policy=feeding_subscriptions_policy)

    async def _process_update(self, *args) -> List[datetime]:

        if self.init:
            if self.initial_delay_seconds>0:
                await sleep(self.initial_delay_seconds)
            else:
                await sleep(self.period_seconds)
            self.init = False
        else:
            await sleep(self.period_seconds)

        return [datetime.now(self.timezone)]

