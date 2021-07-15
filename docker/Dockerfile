#syntax=docker/dockerfile:1.2
ARG V_BASE=3.3.0
FROM opendatacube/geobase-runner:${V_BASE}
ENV LC_ALL=C.UTF-8
ENV PATH="/env/bin:${PATH}"


RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends --allow-change-held-packages \
    # to become test user
    sudo \
    # git is needed for sdist|bdist_wheel
    git \
    # for docs
    make \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 odc \
    && useradd --gid 1000 \
    --uid 1000 \
    --create-home \
    --shell /bin/bash -N odc \
    && adduser odc users \
    && adduser odc sudo \
    && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
    && mkdir /conf \
    && install -d /env -g odc -o odc \
    && install -d /code -g odc -o odc \
    && true


COPY ./with_bootstrap /usr/local/bin
ENTRYPOINT ["/usr/local/bin/with_bootstrap"]
VOLUME ["/code"]
WORKDIR /code

COPY ./rr-odc-tools.in ./constraints.txt ./requirements.txt /conf
USER odc
RUN --mount=type=bind,target=/src \
    --mount=type=cache,target=/home/odc/.cache/pip,uid=1000,gid=1000 \
    (cd /src && tar c .git libs apps ) | (cd /code && tar x) \
    && env-build-tool new_no_index /conf/rr-odc-tools.in /conf/constraints.txt /env /src/docker/wheels \
    && rm -rf /code/*
USER root
