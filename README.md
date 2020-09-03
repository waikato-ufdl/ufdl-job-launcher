# ufdl-job-launcher
Launcher framework for executing jobs in the UFDL framework.

## Requirements

* Python 3.7 or 3.8 (including development headers)

  ```commandline
  sudo apt-get install python3.7 python3.7-dev
  ```

  or

  ```commandline
  sudo apt-get install python3.8 python3.8-dev
  ```

* essential build environment

  ```commandline
  sudo apt-get install build-essential
  ```

## Scripts

* `dev_init.sh` - sets up a development virtual environment


## Tools

The following command-line tools (i.e., entry points) are available:

### ufdl-joblauncher

```
usage: ufdl-joblauncher [-h] [-C FILE] [-c]

Starts the UFDL job-launcher.

optional arguments:
  -h, --help            show this help message and exit
  -C FILE, --config FILE
                        The configuration to use if not the system wide one
                        (/etc/ufdl/job-launcher.conf). (default: None)
  -c, --continuous      For continuous polling for jobs rather than stopping
                        after executing the first one. (default: False)
```

### ufdl-hwinfo

```
usage: ufdl-hwinfo [-h] [-C FILE] [-F FORMAT] [-O FILE]

Outputs UFDL hardware information.

optional arguments:
  -h, --help            show this help message and exit
  -C FILE, --config FILE
                        The configuration to use if not the system wide one
                        (/etc/ufdl/job-launcher.conf). (default: None)
  -F FORMAT, --format FORMAT
                        The format to use for the output of the information.
                        (default: yaml)
  -O FILE, --output FILE
                        The file to store the information in, otherwise stdout
                        is used. (default: None)
```


## Example configuration

An example configuration is available from 
[examples/job-launcher-example.conf](examples/job-launcher-example.conf).


## Executors

Executors are the workhorses in the job-launcher framework that interpret and execute
the jobs that are compatible with a worker node's hardware setup (like GPU available 
and capability). 

The jobs that get executed are based on the job templates defined in the 
[UFDL backend](https://github.com/waikato-ufdl/ufdl-backend). Executors are therefore 
tightly coupled with the job-templates, as these reference the executor class 
and required packages for running the executor.


### Super classes

The following super classes are available

* `ufdl.joblauncher.AbstractJobExecutor` - ancestor for all executors
* `ufdl.joblauncher.AbstractDockerJobExecutor` - for launching docker-image-based jobs


### Implementations

You can find executor implementations in the following repositories:

* [ufdl-job-launcher-plugins](https://github.com/waikato-ufdl/ufdl-job-launcher-plugins)
