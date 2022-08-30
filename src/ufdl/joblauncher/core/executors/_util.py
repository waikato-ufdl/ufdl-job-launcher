import os
import shlex
from typing import Tuple, Union

from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.dataset import clear as dataset_clear

from wai.annotations.core.builder import ConversionPipelineBuilder


def download_dataset(
        context: UFDLServerContext,
        pk: int,
        domain: str,
        output_dir: str,
        options: Union[str, Tuple[str, ...]],
        clear_dataset: bool = False
) -> None:
    """
    Downloads the dataset.

    :param pk:
                The primary key of the dataset to download.
    :param clear_dataset:
                Whether to clear the dataset first.
    :return:
                The archive filename.
    """
    # clear dataset
    if clear_dataset:
        dataset_clear(context, pk)

    source_options = [
        f"from-ufdl-{domain}",
        "--host", context.host,
        "--username", context.username,
        "--password", context.password,
        "--team", "",
        "--project", "",
        "--datasets", f"pk:{pk}"
    ]

    pipeline = ConversionPipelineBuilder.from_options(
        source_options +
        (shlex.split(options) if isinstance(options, str) else list(options))
    )

    cwd = os.getcwd()
    os.chdir(output_dir)
    try:
        pipeline.process()
    finally:
        os.chdir(cwd)
