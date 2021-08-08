#syntax=docker/dockerfile:1.2
ARG V_BASE=3.3.0
FROM opendatacube/geobase-runner:${V_BASE}
ENV LC_ALL=C.UTF-8
ENV PATH="/env/bin:${PATH}"


RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends --allow-change-held-packages \
    # git is needed for sdist|bdist_wheel
    git \
    # for docs
    make \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code

COPY docker/constraints.txt docker/requirements.txt /conf/
RUN --mount=type=bind,target=/src \
    --mount=type=cache,target=/home/odc/.cache/pip,uid=1000,gid=1000 \
    (cd /src && tar c .git libs apps ) | (cd /code && tar x) \
    && env-build-tool new_no_index /conf/requirements.txt /conf/constraints.txt /env /src/docker/wheels
