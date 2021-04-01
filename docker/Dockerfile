# Ubuntu 18.04 with Python 3.7
ARG DOCKER_REGISTRY=public.aml-repo.cms.waikato.ac.nz:443/
FROM ${DOCKER_REGISTRY}ubuntu:bionic

ARG PYTHON=python3.7

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends git-core libglib2.0-0 libsm6 libxrender-dev libxrender1 libxext6 docker && \
    apt-get install -y build-essential ${PYTHON} ${PYTHON}-dev lib${PYTHON}-dev virtualenv && \
    rm -rf /var/lib/apt/lists/*

# Clone and install the UFDL repos
RUN mkdir /ufdl && \
    cd /ufdl && \
    git clone https://github.com/waikato-ufdl/ufdl-json-messages.git && \
    git clone https://github.com/waikato-ufdl/ufdl-python-client.git && \
    git clone https://github.com/waikato-ufdl/ufdl-job-launcher.git && \
    cd /ufdl/ufdl-job-launcher && \
    ./dev_init.sh -y

# Install the Docker CLI
RUN cd / && \
    apt-get update && \
    apt-get install apt-transport-https ca-certificates curl gnupg lsb-release && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install docker-ce-cli && \
    apt-get remove -y apt-transport-https ca-certificates curl gnupg lsb-release && \
    rm -f /usr/share/keyrings/docker-archive-keyring.gpg /etc/apt/sources.list.d/docker.list && \
    rm -rf /var/lib/apt/lists/*

COPY bash.bashrc /etc/bash.bashrc

WORKDIR /ufdl/ufdl-job-launcher

ENTRYPOINT ["./venv.dev/bin/python", "-m", "ufdl.joblauncher.run"]
CMD ["-c", "-C", "./examples/job-launcher-example.conf"]
