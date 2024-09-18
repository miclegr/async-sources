from .source import Source
from asyncio import sleep
from datetime import datetime, timedelta, timezone
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


class ClockSource(Source):

    def __init__(self, period_seconds: float, timezone = timezone.utc, feeding_subscriptions_policy: str = 'on_subscribe'):

        self.period_seconds = period_seconds
        self.timezone = timezone
        self._to_be_replaced = {'microsecond':0}
        if period_seconds // 60 > 0:
            self._to_be_replaced['second'] = 0
        if period_seconds // (60*60) > 0:
            self._to_be_replaced['minute'] = 0

        super().__init__(feeding_subscriptions_policy=feeding_subscriptions_policy)

    async def _process_update(self, *args) -> List[datetime]:

        now = datetime.now(self.timezone)
        then = now.replace(**self._to_be_replaced) + timedelta(seconds=self.period_seconds)
        await sleep((then-now).total_seconds())
        return [then]

