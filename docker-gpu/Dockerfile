# Ubuntu 20.04 with Python 3.8
ARG DOCKER_REGISTRY=public.aml-repo.cms.waikato.ac.nz:443/
FROM ${DOCKER_REGISTRY}nvidia/cuda:11.0.3-cudnn8-devel-ubuntu20.04

ARG PYTHON=python3.8

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends git-core libglib2.0-0 libsm6 libxrender-dev libxrender1 libxext6 docker libgl1 libsndfile1 && \
    apt-get install -y build-essential ${PYTHON} ${PYTHON}-dev lib${PYTHON}-dev virtualenv && \
    rm -rf /var/lib/apt/lists/*

# Clone and install the UFDL repos
RUN mkdir /ufdl && \
    cd /ufdl && \
    git clone https://github.com/waikato-ufdl/ufdl-json-messages.git && \
    git clone https://github.com/waikato-ufdl/ufdl-job-types.git && \
    git clone https://github.com/waikato-ufdl/ufdl-job-contracts.git && \
    git clone https://github.com/waikato-ufdl/ufdl-python-client.git && \
    git clone https://github.com/waikato-ufdl/ufdl-job-launcher.git && \
    git clone https://github.com/waikato-ufdl/ufdl-annotations-plugin.git && \
    cd /ufdl/ufdl-job-launcher && \
    ./dev_init.sh -y

# Install the Docker CLI
RUN cd / && \
    apt-get update && \
    apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y docker-ce-cli && \
    apt-get remove -y apt-transport-https ca-certificates curl gnupg lsb-release && \
    rm -f /usr/share/keyrings/docker-archive-keyring.gpg /etc/apt/sources.list.d/docker.list && \
    rm -rf /var/lib/apt/lists/*

COPY bash.bashrc /etc/bash.bashrc

WORKDIR /ufdl/ufdl-job-launcher

ENTRYPOINT ["./venv.dev/bin/python", "-m", "ufdl.joblauncher.run"]
CMD ["-c", "-C", "./examples/job-launcher-example.conf"]
