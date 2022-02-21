
def dummy_command_progress_parser(cmd_output, last_progress):
    """
    Dummy implementation of a command progress parser for providing feedback to the backend about the progress.
    Doesn't do anything, just returns the last progress.

    :param cmd_output: the command output string to process
    :type cmd_output: str
    :param last_progress: the last reported progress (0-1)
    :type last_progress: float
    :return: returns the progress (0-1)
    :rtype: float
    """
    return last_progress
