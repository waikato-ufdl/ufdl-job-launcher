import time
from typing import List, Union

from ._logging import logger


class SleepSchedule(object):
    """
    Helper class for sleeping according to given schedules.
    """

    def __init__(
            self,
            schedule:  Union[str, List[int]],
            debug: bool = False,
            debug_msg: str = "sleeping for %s seconds"
    ):
        """
        Initializes the instance with the specified schedule.

        :param schedule: the schedule to use (either comma-separated string of integers or list of int)
        :param debug: whether to output debugging messages
        :param debug_msg: the debugging message (use %s as placeholder for the seconds)
        """
        if isinstance(schedule, str):
            schedule = schedule.replace(" ", "").split(",")
        schedule = [int(x) for x in schedule]
        if len(schedule) == 0:
            raise Exception("Schedule has to have at least one element!")
        self._schedule = schedule
        self._current = 0
        self._debug = debug
        self._debug_msg = debug_msg

    @property
    def schedule(self) -> List[int]:
        """
        Returns the underlying schedule.

        :return: the schedule (list of int, ie seconds)
        """
        return self._schedule

    @property
    def debug(self) -> bool:
        """
        Returns whether debug output is on.

        :return: True if debug output is on
        """
        return self._debug

    @property
    def debug_msg(self) -> str:
        """
        Returns the debug message.

        :return: the debug message (%s is used for the seconds)
        :rtype: str
        """
        return self._debug_msg

    @property
    def current(self) -> int:
        """
        Returns the current index in the schedule.

        :return: the schedule index
        """
        return self._current

    def sleep(self) -> None:
        """
        Sleeps the amount of seconds according to the current
        """
        seconds = self.schedule[self.current]
        if self.debug:
            logger().debug(self.debug_msg % str(seconds))
        time.sleep(seconds)

    def reset(self) -> None:
        """
        Resets the schedule.
        """
        self._current = 0

    def next(self) -> None:
        """
        Moves on to the next schedule (if possible, otherwise uses last).
        """
        if self._current + 1 < len(self.schedule):
            self._current += 1

    def __str__(self) -> str:
        """
        Returns a string representation.

        :return: the string representation
        """
        return "schedule=" + str(self.schedule) \
               + ", debug=" + str(self.debug) \
               + ", debug_msg=" + self.debug_msg
