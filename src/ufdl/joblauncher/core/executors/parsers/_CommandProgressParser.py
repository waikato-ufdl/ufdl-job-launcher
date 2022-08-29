from abc import ABC
from typing import Callable


class CommandProgressParser(ABC):
    def parse(self, cmd_output: str, last_progress: float) -> float:
        """
        TODO

        :param cmd_output: the command output string to process
        :param last_progress: the last reported progress (0-1)
        :return: returns the progress (0-1)
        """
        raise NotImplementedError(self.parse.__qualname__)

    @staticmethod
    def from_callable(callable: Callable[[str, float], float]) -> 'CommandProgressParser':
        """
        Creates a parser from a functional definition of parse.

        :param callable:
                    The functional definition of parse.
        :return:
                    The equivalent parser.
        """
        class FunctionalParser(CommandProgressParser):
            def parse(self, cmd_output: str, last_progress: float) -> float:
                return callable(cmd_output, last_progress)

        return FunctionalParser()
