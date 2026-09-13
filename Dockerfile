FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/opt/java \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        openjdk-11-jdk-headless \
        python3 \
        python3-pip \
        python3-dev \
        unzip && \
    ln -s "$(dirname "$(dirname "$(readlink -f /usr/bin/java)")")" "${JAVA_HOME}" && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="${JAVA_HOME}/bin:${PATH}"

ARG USER_UID=1000
ARG USER_GID=1000

RUN (getent group "${USER_GID}" >/dev/null || groupadd --gid "${USER_GID}" app) && \
    useradd --uid "${USER_UID}" --gid "${USER_GID}" \
        --create-home --shell /bin/bash app && \
    mkdir -p /app && \
    chown -R "${USER_UID}:${USER_GID}" /app

RUN python3 -m pip install --no-cache-dir \
        pyspark==3.5.6 \
        delta-spark==3.2.0

WORKDIR /app
ENV HOME=/home/app

USER app

# Keep the container available for an interactive terminal by default.
CMD ["bash"]
