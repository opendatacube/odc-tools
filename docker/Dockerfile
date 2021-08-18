#syntax=docker/dockerfile:1.2
ARG V_BASE=3.3.0
ARG V_PG=12
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
    # for integration tests
    postgresql \
    postgresql-client-${V_PG} \
    postgresql-${V_PG} \
    # for matching directory permissions when running tests as non-root user
    sudo \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 odc \
  && useradd --gid 1000 \
  --uid 1000 \
  --create-home \
  --shell /bin/bash -N odc \
  && adduser odc users \
  && adduser odc sudo \
  && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers \
  && install -d -o odc -g odc /env \
  && install -d -o odc -g odc /code \
  && install -d -o odc -g odc -D /var/run/postgresql /srv/postgresql \
  && true

COPY docker/constraints.txt docker/requirements.txt /conf/
COPY docker/assets/with-bootstrap /usr/local/bin/
COPY docker/assets/with-test-db /usr/local/bin/

USER odc
# Bake in fresh empty datacube db into docker image (owned by odc user)
RUN with-test-db prepare

RUN --mount=type=bind,target=/src \
    --mount=type=cache,target=/home/odc/.cache/pip,uid=1000,gid=1000 \
    (cd /src && tar c libs apps ) | (cd /code && tar x) \
  && env-build-tool new_no_index /conf/requirements.txt /conf/constraints.txt /env /src/docker/wheels \
  && rm -rf /code/* \
  && echo "Done"

USER root

WORKDIR /code
#ENTRYPOINT ["/bin/tini", "-s", "--", "/usr/local/bin/with-bootstrap"]
ENTRYPOINT ["/usr/local/bin/with-bootstrap"]
