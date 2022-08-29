from ._CommandProgressParser import CommandProgressParser


class DummyCommandProgressParser(CommandProgressParser):
    """
    Dummy implementation of a command progress parser for providing feedback to the backend about the progress.
    Doesn't do anything, just returns the last progress.
    """
    def parse(self, cmd_output: str, last_progress: float) -> float:
        return last_progress
