from typing import Optional, Tuple

from ._CommandProgressParser import CommandProgressParser

from wai.json.raw import RawJSONObject


class DummyCommandProgressParser(CommandProgressParser):
    """
    Dummy implementation of a command progress parser for providing feedback to the backend about the progress.
    Doesn't do anything, just returns the last progress.
    """
    def parse(self, cmd_output: str, last_progress: float) -> Tuple[float, Optional[RawJSONObject]]:
        return last_progress, None
