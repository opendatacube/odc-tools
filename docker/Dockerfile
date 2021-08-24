#syntax=docker/dockerfile:1.2
ARG ORG=opendatacube
ARG V_BASE=3.3.0
ARG V_PG=12

FROM ${ORG}/geobase-builder:${V_BASE}
ARG V_PG
ENV LC_ALL=C.UTF-8
ENV PATH="/env/bin:${PATH}"

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing --no-install-recommends --allow-change-held-packages \
    # git is needed for sdist|bdist_wheel
    git \
    # for docs
    make \
    graphviz \
    # for integration tests
    postgresql \
    postgresql-client-${V_PG} \
    postgresql-${V_PG} \
    # for matching directory permissions when running tests as non-root user
    sudo \
    && rm -rf /var/lib/apt/lists/*

RUN install -d -o odc -g odc /env \
  && install -d -o odc -g odc /code \
  && install -d -o odc -g odc -D /var/run/postgresql /srv/postgresql \
  && install -d -o odc -g odc -D /home/odc/.cache/pip \
  && chown -R odc:odc /home/odc/ \
  && true

COPY constraints.txt requirements.txt /conf/
USER odc

RUN --mount=type=cache,target=/home/odc/.cache/pip,uid=1000,gid=1000 \
    --mount=type=cache,target=/wheels,uid=1000,gid=1000 \
  env-build-tool wheels /conf/requirements.txt /conf/constraints.txt /wheels \
  && env-build-tool new_no_index /conf/requirements.txt /conf/constraints.txt /env /wheels


# Bake in fresh empty datacube db into docker image (owned by odc user)
# Need to run after environment setup as it needs datacube
COPY --chown=0:0 assets/with-bootstrap /usr/local/bin/
COPY --chown=0:0 assets/with-test-db /usr/local/bin/
RUN with-bootstrap with-test-db prepare

USER root

WORKDIR /code
ENTRYPOINT ["/bin/tini", "-s", "--", "/usr/local/bin/with-bootstrap"]
