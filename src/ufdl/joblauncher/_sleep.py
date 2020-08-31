import time
from ._logging import logger


class SleepSchedule(object):
    """
    Helper class for sleeping according to given schedules.
    """

    def __init__(self, schedule, debug=False, debug_msg="sleeping for %s seconds"):
        """
        Initializes the instance with the specified schedule.

        :param schedule: the schedule to use (either comma-separated string of integers or list of int)
        :param debug: whether to output debugging messages
        :type debug: bool
        :param debug_msg: the debugging message (use %s as placeholder for the seconds)
        :type debug_msg: str
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
    def schedule(self):
        """
        Returns the underlying schedule.

        :return: the schedule (list of int, ie seconds)
        :rtype: list
        """
        return self._schedule

    @property
    def debug(self):
        """
        Returns whether debug output is on.

        :return: True if debug output is on
        :rtype: bool
        """
        return self._debug

    @property
    def debug_msg(self):
        """
        Returns the debug message.

        :return: the debug message (%s is used for the seconds)
        :rtype: str
        """
        return self._debug_msg

    @property
    def current(self):
        """
        Returns the current index in the schedule.

        :return: the schedule index
        :rtype: int
        """
        return self._current

    def sleep(self):
        """
        Sleeps the amount of seconds according to the current
        :return:
        """
        seconds = self.schedule[self.current]
        if self.debug:
            logger().debug(self.debug_msg % str(seconds))
        time.sleep(seconds)

    def reset(self):
        """
        Resets the schedule.
        """
        self._current = 0

    def next(self):
        """
        Moves on to the next schedule (if possible, otherwise uses last).
        """
        self._current += 1
        if self._current >= len(self.schedule):
            self.reset()

    def __str__(self):
        """
        Returns a string representation.

        :return: the string representation
        :rtype: str
        """
        return "schedule=" + str(self.schedule) \
               + ", debug=" + str(self.debug) \
               + ", debug_msg=" + self.debug_msg
