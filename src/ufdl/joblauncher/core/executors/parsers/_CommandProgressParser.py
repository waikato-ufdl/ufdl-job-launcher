from abc import ABC
from typing import Callable, Optional, Tuple

from wai.json.raw import RawJSONObject


class CommandProgressParser(ABC):
    """
    Object which parses stdout from a running command for progress information.
    """
    def parse(
            self,
            cmd_output: str,
            last_progress: float
    ) -> Tuple[float, Optional[RawJSONObject]]:
        """
        Takes a line of standard output from the running process and parses
        it for progress information.

        :param cmd_output: the command output string to process
        :param last_progress: the last reported progress (0-1)
        :return: returns the progress (0-1), and optionally some JSON progress metadata.
        """
        raise NotImplementedError(self.parse.__qualname__)

    @staticmethod
    def from_callable(
            callable: Callable[[str, float], Tuple[float, Optional[RawJSONObject]]]
    ) -> 'CommandProgressParser':
        """
        Creates a parser from a functional definition of parse.

        :param callable:
                    The functional definition of parse.
        :return:
                    The equivalent parser.
        """
        class FunctionalParser(CommandProgressParser):
            def parse(self, cmd_output: str, last_progress: float) -> Tuple[float, Optional[RawJSONObject]]:
                return callable(cmd_output, last_progress)

        return FunctionalParser()
