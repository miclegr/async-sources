from abc import ABC, abstractmethod, abstractstaticmethod
from datetime import datetime
from typing import Any, Tuple, List
from .source import NoUpdate, Source

class Interval:

    def __init__(self, start_date, end_date, **kwargs) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.attributes = kwargs
        self.store = []

    def add(self, what):
        self.store.append(what)

    def __contains__(self, other: datetime):
        assert isinstance(other, datetime)
        return self.start_date <= other and self.end_date>=other

    def __len__(self):
        return len(self.store)

    def __repr__(self) -> str:
        start = self.start_date.strftime("%Y%m%d %H:%M:%S")
        end = self.end_date.strftime("%Y%m%d %H:%M:%S")
        return f'Interval[{start},{end}]'

class IntervalSource(Source,ABC):

    def _setup_internal_store(self):
        self._active_intervals = []

    @abstractstaticmethod
    def _key_by_time(data) -> datetime:
        pass

    @abstractmethod
    def _update_active_intervals(self, intervals: List[Interval], key: datetime) -> List[Interval]:
        pass

    @staticmethod
    def _add_in_interval(interval: Interval, key: datetime) -> bool:
        return key in interval

    @abstractstaticmethod
    def _is_interval_ready(interval: Interval) -> bool:
        pass

    def _finalize_interval(self, interval: Interval) -> Any:
        return interval

    async def _process_update(self, *args):
        assert len(args) == 1
        data, _ = args[0]

        key = self._key_by_time(data)
        new_intervals = self._update_active_intervals(self._active_intervals, key)
        self._active_intervals.extend(new_intervals)

        updates = []
        to_remove = []

        for interval in self._active_intervals:
            if self._add_in_interval(interval, key):
                interval.add(data)
                if self._is_interval_ready(interval):
                    to_remove.append(interval)
                    updates.append(self._finalize_interval(interval))

        self._active_intervals = [x for x in self._active_intervals if x not in to_remove]

        if len(updates)>0:
            return updates
        else:
            raise NoUpdate

