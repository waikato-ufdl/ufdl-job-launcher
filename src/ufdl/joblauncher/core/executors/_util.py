import os
from typing import List

from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.dataset import clear as dataset_clear

from wai.annotations.core.builder import ConversionPipelineBuilder


def download_dataset(
        context: UFDLServerContext,
        pk: int,
        domain: str,
        output_dir: str,
        options: List[str],
        clear_dataset: bool = False
) -> None:
    """
    Downloads a dataset from the UFDL server into the given directory.

    :param context:
                The context for the server to download from.
    :param pk:
                The primary key of the dataset to download.
    :param domain:
                The domain of the dataset being downloaded.
    :param output_dir:
                The directory to write the dataset into.
    :param options:
                The wai-annotations options for ISPs/writers. Can be prefixed with additional
                source options.
    :param clear_dataset:
                Whether to clear the dataset first.
    :return:
                The archive filename.
    """
    # Clear the dataset, if selected
    if clear_dataset:
        dataset_clear(context, pk)

    # Create the source options for downloading the dataset using the
    # ufdl-annotations-plugin
    source_options = [
        f"from-ufdl-{domain}",
        "--host", context.host,
        "--username", context.username,
        "--password", context.password,
        "--team", "",
        "--project", "",
        "--datasets", f"pk:{pk}"
    ]

    # Create a wai-annotations pipeline from the options
    pipeline = ConversionPipelineBuilder.from_options(source_options + options)

    # Download the dataset to the given directory, changing back to the
    # current directory once complete
    cwd = os.getcwd()
    os.chdir(output_dir)
    try:
        pipeline.process()
    finally:
        os.chdir(cwd)
