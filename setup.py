from setuptools import setup, find_namespace_packages

def _read(f) -> bytes:
    """
    Reads in the content of the file.
    :param f: the file to read
    :type f: str
    :return: the content
    :rtype: str
    """
    return open(f, 'rb').read()


setup(
    name="ufdl.joblauncher",
    description="Launches job as part of the UFDL framework.",
    long_description=(
        _read('DESCRIPTION.rst') + b'\n' +
        _read('CHANGES.rst')).decode('utf-8'),
    url="https://github.com/waikato-ufdl/ufdl-job-launcher",
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 3',
    ],
    license='Apache 2.0 License',
    package_dir={
        '': 'src'
    },
    packages=find_namespace_packages(where='src'),
    namespace_packages=[
        "ufdl",
    ],
    version="0.0.1",
    author='Peter Reutemann',
    author_email='fracpete@waikato.ac.nz',
    install_requires=[
        "wai.annotations",
        "ufdl.pythonclient",
        "ufdl.json-messages",
        "wai.lazypip",
        "pika",
        "tensorflow",
    ],
)
